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
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/pgzip"
	"golang.org/x/sync/errgroup"
)

type fileJob struct {
	index int
	path  string
	info  fs.FileInfo
	pr    *io.PipeReader
	pw    *io.PipeWriter
}

func main() {
	// ---- Flags ----
	src := flag.String("src", "", "Source directory (required)")
	outTpl := flag.String("out", "", "Output path template with time placeholders (required), e.g. backups/%F_%T.tar.gz")
	keep := flag.Int("keep", 0, "Keep exactly N newest archives by mtime (0 disables pruning)")
	flag.Parse()

	if *src == "" || *outTpl == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -src <dir> -out <template> [-keep N]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	// Expand output path from template with time placeholders
	outPath := strftimeLike(*outTpl, time.Now())
	if dir := filepath.Dir(outPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fail(err)
		}
	}

	// 1) Collect files
	files, err := collectFiles(*src)
	if err != nil {
		fail(err)
	}
	if len(files) == 0 {
		fmt.Println("No files found.")
		return
	}

	// 2) Open output + pgzip writer
	outF, err := os.Create(outPath)
	if err != nil {
		fail(err)
	}
	defer outF.Close()

	gzw := pgzip.NewWriter(outF)                     // <-- fixed: only 1 return value
	gzw.SetConcurrency(1<<20, runtime.GOMAXPROCS(0)) // 1 MiB blocks, N workers
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	// 3) Context + errgroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)

	// 4) Worker pool
	workers := max(2, runtime.GOMAXPROCS(0))
	jobs := make(chan *fileJob, workers*2)

	// Buffer pool
	var bufPool = sync.Pool{
		New: func() any { b := make([]byte, 256*1024); return &b }, // 256 KiB
	}

	// 5) Workers: file -> pipe
	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			for job := range jobs {
				if err := streamFileToPipe(ctx, job, &bufPool); err != nil {
					job.pw.CloseWithError(err)
					return err
				}
				job.pw.Close()
			}
			return nil
		})
	}

	// 6) Writer: headers + stream pipes to tar (in order)
	eg.Go(func() error {
		for idx := 0; idx < len(files); idx++ {
			job := files[idx]

			hdr, err := tar.FileInfoHeader(job.info, "")
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(*src, job.path)
			hdr.Name = filepath.ToSlash(rel)
			hdr.ModTime = job.info.ModTime().UTC()

			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}

			buf := *bufPool.Get().(*[]byte)
			if _, err := io.CopyBuffer(tw, job.pr, buf); err != nil {
				job.pr.CloseWithError(err)
				return err
			}
			bufPool.Put(&buf)
			job.pr.Close()
		}
		return nil
	})

	// 7) Enqueue jobs
	eg.Go(func() error {
		for i := range files {
			pr, pw := io.Pipe()
			files[i].pr, files[i].pw = pr, pw
			select {
			case jobs <- &files[i]:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		close(jobs)
		return nil
	})

	// 8) Wait
	if err := eg.Wait(); err != nil {
		_ = tw.Close()
		_ = gzw.Close()
		_ = outF.Close()
		removePartial(outPath)
		fail(err)
	}

	// fsync (best-effort) — <-- fixed: no type assertion needed
	_ = outF.Sync()

	fmt.Printf("Wrote %d files to %s\n", len(files), outPath)

	// 9) Pruning (keep newest N; always keep current)
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
	if !job.info.Mode().IsRegular() {
		return nil
	}
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

func collectFiles(root string) ([]fileJob, error) {
	var out []fileJob
	i := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out = append(out, fileJob{index: i, path: p, info: info})
		i++
		return nil
	})
	return out, err
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

	sort.Slice(items, func(i, j int) bool { return items[i].mt.After(items[j].mt) }) // newest first
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

// ---- misc ----

func removePartial(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}
