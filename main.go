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
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type fileJob struct {
	path string
	info fs.FileInfo
	pr   *io.PipeReader
	pw   *io.PipeWriter
}

func main() {
	// ---- Flags ----
	src := flag.String("src", "", "Source directory (required)")
	outTpl := flag.String("out", "", "Output path template with time placeholders (required), e.g. backups/%F_%T.tar.gz")
	keep := flag.Int("keep", 0, "Keep exactly N newest archives by mtime (0 disables pruning)")

	logFile := flag.String("logfile", "", "Optional log file path (defaults to stderr)")
	logJSON := flag.Bool("log-json", true, "Log as JSON (true) or pretty console (false)")
	logLevel := flag.String("log-level", "info", "Log level: debug|info|warn|error")
	logInterval := flag.Duration("log-interval", 10*time.Second, "Periodic summary interval (0 disables)")
	logHeaders := flag.Bool("log-headers", false, "Also log non-regular entries (dirs/symlinks)")
	flag.Parse()

	if *src == "" || *outTpl == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -src <dir> -out <template> [-keep N] [-logfile path] [-log-json bool] [-log-level L] [-log-interval 10s] [-log-headers]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	// ---- Logger Setup (zerolog) ----
	var out io.Writer = os.Stderr
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "logfile open failed (%v), using stderr\n", err)
		} else {
			out = f
			defer f.Close()
		}
	}
	if *logJSON {
		zerolog.TimeFieldFormat = time.RFC3339Nano
		log.Logger = zerolog.New(out).With().Timestamp().Logger()
	} else {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: out, TimeFormat: "15:04:05"})
	}
	switch strings.ToLower(*logLevel) {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// Final output + temp partial
	outPath := strftimeLike(*outTpl, time.Now())
	tmpPath := outPath + ".partial"
	if dir := filepath.Dir(outPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fail(err)
		}
	}

	// Context with signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	eg, ctx := errgroup.WithContext(ctx)

	// Open temp writer
	tmpF, err := os.Create(tmpPath)
	if err != nil {
		fail(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tmpF.Close()
			_ = os.Remove(tmpPath)
			log.Warn().Str("tmp", tmpPath).Msg("removed partial file")
		}
	}()

	gzw := pgzip.NewWriter(tmpF)
	gzw.SetConcurrency(1<<20, runtime.GOMAXPROCS(0)) // 1 MiB blocks, N workers
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	// ---- Metrics for periodic summaries ----
	var filesDone int64
	var bytesRead int64
	var bytesPadded int64
	startTime := time.Now()

	// Periodic summary ticker (non-blocking)
	if *logInterval > 0 {
		interval := *logInterval
		eg.Go(func() error {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			var lastBytes int64
			var lastTime = time.Now()
			for {
				select {
				case <-ctx.Done():
					return nil
				case t := <-ticker.C:
					total := atomic.LoadInt64(&bytesRead)
					deltaBytes := total - lastBytes
					deltaSecs := t.Sub(lastTime).Seconds()
					speed := float64(deltaBytes)
					if deltaSecs > 0 {
						speed = speed / deltaSecs
					}
					log.Info().
						Int64("files", atomic.LoadInt64(&filesDone)).
						Str("bytes", humanBytes(total)).
						Str("padded", humanBytes(atomic.LoadInt64(&bytesPadded))).
						Str("speed", humanBytes(int64(speed))+"/s").
						Str("elapsed", time.Since(startTime).Truncate(time.Second).String()).
						Msg("progress")
					lastBytes = total
					lastTime = t
				}
			}
		})
	}

	// Channels & pools
	workCh := make(chan *fileJob, runtime.GOMAXPROCS(0)*2)
	orderCh := make(chan *fileJob, runtime.GOMAXPROCS(0)*2)
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

	// Writer: headers + exact bytes from pipe (ordered)
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

			// Only regular files have data
			if !job.info.Mode().IsRegular() || hdr.Size == 0 {
				if *logHeaders {
					log.Debug().Str("path", name).Str("type", entryType(job.info)).Msg("header")
				}
				job.pr.Close()
				continue
			}

			// Copy exactly hdr.Size bytes; record metrics
			start := time.Now()
			buf := *bufPool.Get().(*[]byte)
			read, padded, cErr := copyExactly(ctx, tw, job.pr, hdr.Size, buf)
			bufPool.Put(&buf)
			job.pr.Close()

			atomic.AddInt64(&bytesRead, read)
			if padded > 0 {
				atomic.AddInt64(&bytesPadded, padded)
			}
			atomic.AddInt64(&filesDone, 1)

			evt := log.Info()
			if cErr != nil {
				evt = log.Error().Err(cErr)
			}
			evt.Str("path", name).
				Int64("size", hdr.Size).
				Int64("read", read).
				Int64("padded", padded).
				Str("elapsed", time.Since(start).Truncate(time.Millisecond).String()).
				Msg("file")
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

	// Wait (will cancel on signal)
	if err := eg.Wait(); err != nil {
		_ = tw.Close()
		_ = gzw.Close()
		_ = tmpF.Close()
		fail(err) // deferred cleanup removes partial
	}

	// Flush & fsync
	if err := tw.Close(); err != nil {
		fail(err)
	}
	if err := gzw.Close(); err != nil {
		fail(err)
	}
	if err := tmpF.Sync(); err != nil {
		fail(err)
	}
	if err := tmpF.Close(); err != nil {
		fail(err)
	}

	// Rename to final
	if err := os.Rename(tmpPath, outPath); err != nil {
		fail(fmt.Errorf("rename %q -> %q failed: %w", tmpPath, outPath, err))
	}
	committed = true

	// Final summary
	log.Info().
		Int64("files", atomic.LoadInt64(&filesDone)).
		Str("bytes", humanBytes(atomic.LoadInt64(&bytesRead))).
		Str("padded", humanBytes(atomic.LoadInt64(&bytesPadded))).
		Str("elapsed", time.Since(startTime).Truncate(time.Second).String()).
		Str("output", outPath).
		Msg("archive complete")

	// Prune after success
	if *keep > 0 {
		pattern := templateToGlob(*outTpl)
		if pattern == "" {
			pattern = outPath
		}
		deleted, skippedCurrent, perr := pruneKeepExactlyNewestByMTime(pattern, *keep, outPath)
		if perr != nil {
			log.Error().Err(perr).Msg("prune failed")
		} else {
			log.Info().
				Int("kept", *keep).
				Int("deleted", deleted).
				Int("skipped_current", skippedCurrent).
				Str("pattern", pattern).
				Msg("prune done")
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

// copyExactly copies exactly want bytes, padding zeros if src ends early.
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

// ---- utils ----

func entryType(info fs.FileInfo) string {
	m := info.Mode()
	switch {
	case m.IsDir():
		return "dir"
	case m.IsRegular():
		return "file"
	case m&os.ModeSymlink != 0:
		return "symlink"
	default:
		return "other"
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
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
			log.Warn().Err(remErr).Str("path", it.path).Msg("prune remove failed")
			continue
		}
		deleted++
	}
	return deleted, skippedCurrent, err
}

func fail(err error) {
	log.Error().Err(err).Msg("fatal")
	os.Exit(1)
}
