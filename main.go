// main.go
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
	"syscall"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// ---- Flags ----
	src := flag.String("src", "", "Source directory (required)")
	outTpl := flag.String("out", "", "Output path template with time placeholders (required), e.g. backups/%F_%T.tar.gz")
	keep := flag.Int("keep", 0, "Keep exactly N newest archives by mtime (0 disables pruning)")

	logFile := flag.String("logfile", "", "Optional log file path (defaults to stderr)")
	logJSON := flag.Bool("log-json", true, "Log as JSON (true) or pretty console (false)")
	logLevel := flag.String("log-level", "info", "Log level: debug|info|warn|error")
	logInterval := flag.Duration("log-interval", 15*time.Second, "Periodic summary interval (0 disables)")

	// Performance- und RAM-Tuning (Defaults für ~128 MiB RAM)
	pgzLevel := flag.Int("pgzip-level", 3, "pgzip compression level (1-9)")
	pgzBlock := flag.Int("pgzip-block", 1<<20, "pgzip block size in bytes (default 1MiB)")
	pgzBlocks := flag.Int("pgzip-blocks", 8, "pgzip in-flight blocks (default 8)")
	bufSize := flag.Int("bufsize", 512<<10, "copy buffer size in bytes (default 512KiB)")
	flag.Parse()

	if *src == "" || *outTpl == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -src DIR -out TEMPLATE [options]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	// ---- Logger ----
	var out io.Writer = os.Stderr
	if *logFile != "" {
		if f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			out = f
			defer f.Close()
		} else {
			fmt.Fprintf(os.Stderr, "logfile open failed (%v), using stderr\n", err)
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

	// ---- Pfade (partial + atomisches Rename) ----
	outPath := strftimeLike(*outTpl, time.Now())
	tmpPath := outPath + ".partial"
	if dir := filepath.Dir(outPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fail(err)
		}
	}

	// ---- Context: Abbruch via CTRL-C/TERM ----
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ---- Ausgabe öffnen ----
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

	// ---- pgzip + tar ----
	gzw, err := pgzip.NewWriterLevel(tmpF, clamp(*pgzLevel, 1, 9))
	if err != nil {
		fail(err)
	}
	blk := clamp(*pgzBlock, 256<<10, 1<<20)              // 256KiB..1MiB
	bls := clamp(*pgzBlocks, 2, 8*runtime.GOMAXPROCS(0)) // 2..8xCores
	gzw.SetConcurrency(blk, bls)
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	// ---- Periodische Zusammenfassung ----
	start := time.Now()
	type totals struct{ files, bytes, padded int64 }
	t := totals{}
	if *logInterval > 0 {
		ticker := time.NewTicker(*logInterval)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					log.Info().
						Int64("files", t.files).
						Str("bytes", humanBytes(t.bytes)).
						Str("padded", humanBytes(t.padded)).
						Str("elapsed", time.Since(start).Truncate(time.Second).String()).
						Msg("progress")
				}
			}
		}()
	}

	// ---- Walk & Write (Writer-Pull, keine Pipes/Worker) ----
	err = filepath.WalkDir(*src, func(p string, d fs.DirEntry, inErr error) error {
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

		name := filepath.ToSlash(rel)
		if info.IsDir() && !strings.HasSuffix(name, "/") {
			name += "/"
		}

		var linkname string
		if info.Mode()&os.ModeSymlink != 0 {
			if target, err := os.Readlink(p); err == nil {
				linkname = target
			}
		}

		hdr, err := tar.FileInfoHeader(info, linkname)
		if err != nil {
			return err
		}
		hdr.Name = name
		hdr.ModTime = info.ModTime().UTC()

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		// Nur Regular Files haben Daten
		if !info.Mode().IsRegular() || hdr.Size == 0 {
			log.Debug().Str("path", name).Str("type", entryType(info)).Msg("header")
			return nil
		}

		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()

		buf := make([]byte, clamp(*bufSize, 64<<10, 32<<20)) // 64KiB..32MiB
		startFile := time.Now()
		read, padded, cErr := copyExactly(ctx, tw, f, hdr.Size, buf)
		_ = f.Close()

		t.files++
		t.bytes += read
		t.padded += padded

		ev := log.Info()
		if cErr != nil {
			ev = log.Error().Err(cErr)
		}
		ev.Str("path", name).
			Int64("size", hdr.Size).
			Int64("read", read).
			Int64("padded", padded).
			Str("elapsed", time.Since(startFile).Truncate(time.Millisecond).String()).
			Msg("file")
		return cErr
	})
	if err != nil {
		fail(err) // deferred cleanup entfernt .partial
	}

	// ---- Flush & fsync, dann atomisches Rename ----
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
	if err := os.Rename(tmpPath, outPath); err != nil {
		fail(fmt.Errorf("rename %q -> %q failed: %w", tmpPath, outPath, err))
	}
	committed = true

	// Final Summary
	log.Info().
		Int64("files", t.files).
		Str("bytes", humanBytes(t.bytes)).
		Str("padded", humanBytes(t.padded)).
		Str("elapsed", time.Since(start).Truncate(time.Second).String()).
		Str("output", outPath).
		Msg("archive complete")

	// ---- Pruning ----
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

// ---- Copy-Helpers ----

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

// ---- Utils ----

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

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
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

// ---- Time-Substitutions & Pruning ----

func strftimeLike(tpl string, t time.Time) string {
	rep := []string{
		"%%", "%",
		"%Y", "2006", "%y", "06",
		"%m", "01", "%d", "02",
		"%H", "15", "%M", "04", "%S", "05",
		"%F", "2006-01-02", "%T", "15:04:05",
		"%:z", "-07:00", "%z", "-0700",
	}
	r := strings.NewReplacer(rep...)
	return t.Format(r.Replace(tpl))
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
