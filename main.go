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
	flag.Parse()

	if *src == "" || *outTpl == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -src <dir> -out <template> [-keep N]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	// Expand output path from template
	outPath := strftimeLike(*outTpl, time.Now())
	if dir := filepath.Dir(outPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fail(err)
		}
	}

	// Open output + pgzip writer
	outF, err := os.Create(outPath)
	if err != nil {
		fail(err)
	}
	defer outF.Close()

	gzw := pgzip.NewWriter(outF)
	gzw.SetConcurrency(1<<20, runtime.GOMAXPROCS(0)) // 1 MiB Blöcke, N Worker
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	// Context + errgroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)

	// Channels: workCh für Worker, orderCh für den Writer (Erhalt der Reihenfolge)
	workCh := make(chan *fileJob, runtime.GOMAXPROCS(0)*2)
	orderCh := make(chan *fileJob, runtime.GOMAXPROCS(0)*2)

	// Buffer-Pool
	var bufPool = sync.Pool{
		New: func() any { b := make([]byte, 256*1024); return &b }, // 256 KiB
	}

	// Worker starten – Datei -> Pipe (parallel)
	workers := max(2, runtime.GOMAXPROCS(0))
	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			for job := range workCh {
				// nur Regular Files streamen; bei anderen Typen nur Header (Writer-Seite)
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

	// Writer: nacheinander Header schreiben & exakt die erwarteten Bytes aus Pipe lesen
	eg.Go(func() error {
		for job := range orderCh {
			rel, _ := filepath.Rel(*src, job.path)
			name := filepath.ToSlash(rel)

			// Header (inkl. erwarteter Größe für Regular Files)
			var linkname string
			if job.info.Mode()&os.ModeSymlink != 0 {
				// Ziel des Symlinks ermitteln
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

			// Nur Regular Files haben Daten; sicherstellen, dass GENAU hdr.Size Bytes ins TAR gehen.
			if job.info.Mode().IsRegular() && hdr.Size > 0 {
				buf := *bufPool.Get().(*[]byte)
				if err := copyExactly(ctx, tw, job.pr, hdr.Size, buf); err != nil {
					bufPool.Put(&buf)
					job.pr.CloseWithError(err)
					return err
				}
				bufPool.Put(&buf)
			}
			job.pr.Close()
		}
		return nil
	})

	// Walker: streamt „on the fly“ — kein Vorab-Collect
	eg.Go(func() error {
		defer close(workCh)
		defer close(orderCh)
		err := filepath.WalkDir(*src, func(p string, d fs.DirEntry, inErr error) error {
			if inErr != nil {
				return inErr
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			// Root relativ "." nicht als Eintrag erzwingen (optional)
			rel, _ := filepath.Rel(*src, p)
			if rel == "." {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}

			// Pipe je Eintrag
			pr, pw := io.Pipe()
			job := &fileJob{path: p, info: info, pr: pr, pw: pw}

			// in fester Reihenfolge an den Writer…
			orderCh <- job
			// …und parallel in die Worker-Queue (die ggf. nichts tun bei Dir/Symlink)
			workCh <- job
			return nil
		})
		return err
	})

	// warten
	if err := eg.Wait(); err != nil {
		_ = tw.Close()
		_ = gzw.Close()
		_ = outF.Close()
		removePartial(outPath)
		fail(err)
	}

	_ = outF.Sync()
	fmt.Printf("Wrote stream to %s\n", outPath)

	// Pruning
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

// --- Helpers ---

// Datei blockweise in die Pipe streamen
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

// Liest GENAU want Bytes von src und schreibt nach dst; bei Short-Read wird mit Nullen aufgefüllt.
// So bleibt das TAR-Layout konsistent, selbst wenn eine Datei sich währenddessen verkürzt.
func copyExactly(ctx context.Context, dst io.Writer, src io.Reader, want int64, buf []byte) error {
	var done int64
	for done < want {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
				return ew
			}
			if nw != nr {
				return io.ErrShortWrite
			}
			done += int64(nw)
			continue
		}
		if er != nil {
			if errors.Is(er, io.EOF) {
				// auffüllen
				if remain := want - done; remain > 0 {
					if err := zeroPad(dst, remain); err != nil {
						return err
					}
					done = want
					return nil
				}
				return nil
			}
			return er
		}
	}
	return nil
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

// --- Zeit-Platzhalter & Pruning (wie zuvor) ---

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

func removePartial(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}
