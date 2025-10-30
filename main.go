package main

import (
	"archive/tar"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/klauspost/pgzip"
	"golang.org/x/sync/errgroup"
)

type fileJob struct {
	path string
	info fs.FileInfo
	pr   *io.PipeReader
	pw   *io.PipeWriter
}

type logEvent struct {
	Kind    string
	Path    string
	Size    int64
	Read    int64
	Padded  int64
	Elapsed time.Duration
	Err     error
}

func main() {
	// ---- Flags ----
	src := flag.String("src", "", "Source directory (required)")
	outTpl := flag.String("out", "", "Output path template with time placeholders (required), e.g. backups/%F_%T.tar.gz")
	keep := flag.Int("keep", 0, "Keep exactly N newest archives by mtime (0 disables pruning)")
	logFile := flag.String("logfile", "", "Optional log file path (defaults to stderr)")
	flag.Parse()

	if *src == "" || *outTpl == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -src <dir> -out <template> [-keep N] [-logfile path]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	// Final output name and temp (partial) name
	outPath := strftimeLike(*outTpl, time.Now())
	tmpPath := outPath + ".partial"
	if dir := filepath.Dir(outPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fail(err)
		}
	}

	// Context cancels on SIGINT/SIGTERM -> ensures cleanup
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	eg, ctx := errgroup.WithContext(ctx)

	// Open temp output; ensure removal on any failure/abort
	tmpF, err := os.Create(tmpPath)
	if err != nil {
		fail(err)
	}
	committed := false
	defer func() {
		// If we didn't rename successfully, remove partial
		if !committed {
			_ = tmpF.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	gzw := pgzip.NewWriter(tmpF)
	gzw.SetConcurrency(1<<20, runtime.GOMAXPROCS(0)) // 1 MiB blocks, N workers
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	// ----- Async logger -----
	logCh := make(chan logEvent, 2048)
	var dropped uint64
	eg.Go(func() error {
		var w io.Writer = os.Stderr
		if *logFile != "" {
			if lf, e := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); e == nil {
				defer lf.Close()
				w = lf
			} else {
				fmt.Fprintf(os.Stderr, "logfile open failed (%v), using stderr\n", e)
			}
		}
		for ev := range logCh {
			switch ev.Kind {
			case "header":
				typ := "other"
				if strings.HasSuffix(ev.Path, "/") {
					typ = "dir"
				}
				fmt.Fprintf(w, "header: %s type=%s\n", ev.Path, typ)
			case "copy":
				if ev.Err != nil {
					fmt.Fprintf(w, "copy:   %s size=%d read=%d padded=%d elapsed=%s ERROR=%v\n",
						ev.Path, ev.Size, ev.Read, ev.Padded, ev.Elapsed.Truncate(time.Millisecond), ev.Err)
				} else {
					fmt.Fprintf(w, "copy:   %s size=%d read=%d padded=%d elapsed=%s\n",
						ev.Path, ev.Size, ev.Read, ev.Padded, ev.Elapsed.Truncate(time.Millisecond))
				}
			default:
				fmt.Fprintf(w, "log: %s\n", ev.Path)
			}
		}
		if d := atomic.LoadUint64(&dropped); d > 0 {
			fmt.Fprintf(w, "log: dropped %d events (logger buffer full)\n", d)
		}
		return nil
	})
	logAsync := func(ev logEvent) {
		select {
		case logCh <- ev:
		default:
			atomic.AddUint64(&dropped, 1)
		}
	}

	// Work & order channels
	workCh := make(chan *fileJob, runtime.GOMAXPROCS(0)*2)
	orderCh := make(chan *fileJob, runtime.GOMAXPROCS(0)*2)

	// Buffer pool
	var bufPool = sync.Pool{New: func() any { b := make([]byte, 256*1024); return &b }}

	// Workers: stream file -> pipe
	workers := max(2, runtime.GOMAXPROCS(0))
	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			for job := range workCh {
				if job.info.Mode().IsRegular() {
					if err := streamFileToPipe(ctx, job, &bufPool); err != nil {
						job.pw.CloseWithError(err)
						return err
					}
				}
				job.pw.Close()
			}
			return nil
		})
	}

	// Writer: headers + exact bytes from pipe in order
	eg.Go(func() error {
		for job := range orderCh {
			rel, _ := filepath.Rel(*src, job.path)
			name := filepath.ToSlash(rel)
			if job.info.IsDir() && !strings.HasSuffix(name, "/") {
				name += "/"
			}

			var linkname string
			if job.info.Mode()&os.ModeSymlink != 0 {
				if target, err := os.Readlink(job.path); err == nil {
					linkname = target
				}
			}
			hdr, err := tar.FileInfoHeader(job.info, linkname)
			if err != nil {
				return err
			}
			hdr.Name = name
			hdr.ModTime = job.info.ModTime().UTC()

			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}

			if !job.info.Mode().IsRegular() || hdr.Size == 0 {
				logAsync(logEvent{Kind: "header", Path: name})
				job.pr.Close()
				continue
			}

			start := time.Now()
			buf := *bufPool.Get().(*[]byte)
			read, padded, cErr := copyExactly(ctx, tw, job.pr, hdr.Size, buf)
			bufPool.Put(&buf)
			job.pr.Close()

			logAsync(logEvent{
				Kind:    "copy",
				Path:    name,
				Size:    hdr.Size,
				Read:    read,
				Padded:  padded,
				Elapsed: time.Since(start),
				Err:     cErr,
			})
			if cErr != nil {
				return cErr
			}
		}
		return nil
	})

	// Walker: on-the-fly
	eg.Go(func() error {
		defer close(workCh)
		defer close(orderCh)
		return filepath.WalkDir(*src, func(p string, d fs.DirEntry, inErr error) error {
			if inErr != nil {
				return inErr
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			rel, _ := filepath.Rel(*src, p)
			if rel == "." {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			pr, pw := io.Pipe()
			job := &fileJob{path: p, info: info, pr: pr, pw: pw}
			orderCh <- job
			workCh <- job
			return nil
		})
	})

	// Wait for pipeline (cancels on signal)
	if err := eg.Wait(); err != nil {
		// close logger and exit; deferred cleanup removes tmp
		close(logCh)
		fail(err)
	}

	// Flush & fsync, close writers/file
	if err := tw.Close(); err != nil {
		close(logCh)
		fail(err)
	}
	if err := gzw.Close(); err != nil {
		close(logCh)
		fail(err)
	}
	if err := tmpF.Sync(); err != nil {
		close(logCh)
		fail(err)
	}
	if err := tmpF.Close(); err != nil {
		close(logCh)
		fail(err)
	}

	// Logger beenden
	close(logCh)

	// Atomic rename to final name (same filesystem)
	if err := os.Rename(tmpPath, outPath); err != nil {
		fail(fmt.Errorf("rename %q -> %q failed: %w", tmpPath, outPath, err))
	}
	committed = true

	fmt.Printf("Wrote stream to %s\n", outPath)

	// Prune after successful commit
	if *keep > 0 {
		pattern := templateToGlob(*outTpl)
		if pattern == "" {
			pattern = outPath
		}
		deleted, skippedCurrent, perr := pruneKeepExactlyNewestByMTime(pattern, *keep, outPath)
		if perr != nil {
			fmt.Fprintf(os.Stderr, "prune error: %v\n", perr)
		} else {
			fmt.Printf("Prune: kept=%d, deleted=%d, skipped(current)=%d, pattern=%q\n", *keep, deleted, skippedCurrent, pattern)
		}
	}
}

// ---- streaming helpers ----

func streamFileToPipe(ctx context.Context, job *fileJob, bufPool *sync.Pool) error {
	f, err := os.Open(job.path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := *bufPool.Get().(*[]byte)
	defer bufPool.Put(&buf)

	_, err = copyWithContext(ctx, job.pw, f, buf)
	return err
}

func copyExactly(ctx context.Context, dst io.Writer, src io.Reader, want int64, buf []byte) (read int64, padded int64, err error) {
	var done int64
	for done < want {
		select {
		case <-ctx.Done():
			return read, padded, ctx.Err()
		default:
		}
		remain := want - done
		if int64(len(buf)) > remain {
			buf = buf[:remain]
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			if ew != nil {
				return read, padded, ew
			}
			if nw != nr {
				return read, padded, io.ErrShortWrite
			}
			done += int64(nw)
			read += int64(nw)
			continue
		}
		if er != nil {
			if errors.Is(er, io.EOF) {
				if remain := want - done; remain > 0 {
					if err := zeroPad(dst, remain); err != nil {
						return read, padded, err
					}
					padded += remain
					done = want
				}
				return read, padded, nil
			}
			return read, padded, er
		}
	}
	return read, padded, nil
}

func zeroPad(w io.Writer, n int64) error {
	const zsize = 64 << 10
	var z [zsize]byte
	for n > 0 {
		chunk := n
		if chunk > zsize {
			chunk = zsize
		}
		wrote, err := w.Write(z[:chunk])
		if err != nil {
			return err
		}
		if wrote != int(chunk) {
			return io.ErrShortWrite
		}
		n -= chunk
	}
	return nil
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader, buf []byte) (int64, error) {
	var written int64
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				return written, ew
			}
			if nw != nr {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if errors.Is(er, io.EOF) {
				break
			}
			return written, er
		}
	}
	return written, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ---- time substitutions & pruning ----

func strftimeLike(tpl string, t time.Time) string {
	rep := []string{
		"%%", "%",
		"%Y", "2006",
		"%y", "06",
		"%m", "01",
		"%d", "02",
		"%H", "15",
		"%M", "04",
		"%S", "05",
		"%F", "2006-01-02",
		"%T", "15:04:05",
		"%:z", "-07:00",
		"%z", "-0700",
	}
	r := strings.NewReplacer(rep...)
	layout := r.Replace(tpl)
	return t.Format(layout)
}

func templateToGlob(tpl string) string {
	var b strings.Builder
	runes := []rune(tpl)
	for i := 0; i < len(runes); i++ {
		if runes[i] == '%' {
			b.WriteRune('*')
			if i+1 < len(runes) {
				i++
				if i+1 < len(runes) && runes[i] == ':' {
					i++
				}
			}
			continue
		}
		b.WriteRune(runes[i])
	}
	return b.String()
}

func pruneKeepExactlyNewestByMTime(pattern string, keep int, current string) (deleted int, skippedCurrent int, err error) {
	if keep < 0 {
		keep = 0
	}
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid pattern %q: %w", pattern, err)
	}
	type item struct {
		path string
		mt   time.Time
	}
	var items []item
	for _, p := range matches {
		fi, e := os.Stat(p)
		if e != nil || fi.IsDir() {
			continue
		}
		items = append(items, item{path: p, mt: fi.ModTime()})
	}
	if len(items) == 0 || keep == 0 {
		return 0, 0, nil
	}
	sort.Slice(items, func(i, j int) bool { return items[i].mt.After(items[j].mt) })
	if len(items) <= keep {
		return 0, 0, nil
	}

	keepSet := make(map[int]struct{}, keep)
	for i := 0; i < keep && i < len(items); i++ {
		keepSet[i] = struct{}{}
	}

	// ensure current kept
	if current != "" {
		absCur, _ := filepath.Abs(current)
		curIdx := -1
		for i := range items {
			absPath, _ := filepath.Abs(items[i].path)
			if absCur == absPath {
				curIdx = i
				break
			}
		}
		if curIdx >= 0 {
			if _, ok := keepSet[curIdx]; !ok {
				oldestIdx := -1
				for i := range keepSet {
					if i > oldestIdx {
						oldestIdx = i
					}
				}
				if oldestIdx >= 0 {
					delete(keepSet, oldestIdx)
				}
				keepSet[curIdx] = struct{}{}
			}
		}
	}
	for i, it := range items {
		if _, keep := keepSet[i]; keep {
			continue
		}
		if current != "" {
			absCur, _ := filepath.Abs(current)
			absPath, _ := filepath.Abs(it.path)
			if absCur == absPath {
				skippedCurrent++
				continue
			}
		}
		if remErr := os.Remove(it.path); remErr != nil {
			err = remErr
			fmt.Fprintf(os.Stderr, "prune: failed to remove %s: %v\n", it.path, remErr)
			continue
		}
		deleted++
	}
	return deleted, skippedCurrent, err
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}
