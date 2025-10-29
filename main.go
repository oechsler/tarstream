// main.go
package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

var errAborted = errors.New("aborted")

type changePolicy string

const (
	polSnapshot changePolicy = "snapshot"
	polSkip     changePolicy = "skip"
	polFail     changePolicy = "fail"
)

type prescanMode string

const (
	preNone   prescanMode = "none"
	preFast   prescanMode = "fast"
	preStable prescanMode = "stable"
)

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func main() {
	// basics
	src := flag.String("src", "", "Source directory")
	out := flag.String("out", "-", "Output file or '-' for stdout")
	verbose := flag.Bool("v", false, "Verbose: log file names")

	// flapping prevention
	stability := flag.String("stability", "on", "Flapping prevention: on|off")
	stabMS := flag.Int("stability-ms", 200, "Stability window per check (ms)")
	retries := flag.Int("retries", -1, "Max retries to obtain a stable snapshot (-1 = infinite)")
	onChange := flag.String("on-change", "snapshot", "Policy for changing files: snapshot|skip|fail")

	// progress & totals
	progress := flag.String("progress", "10s", "Per-file progress interval (e.g. 5s, 30s; 0 to disable interim updates)")
	inline := flag.Bool("inline", false, "Inline progress on a single line (carriage return)")
	prescan := flag.String("prescan", "none", "Prescan mode for total size: none|fast|stable")

	// simple pruning
	keep := flag.Int("keep", 0, "Keep newest N archives by mtime (0 disables pruning)")

	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetFlags(0)

	if *src == "" {
		fail(fmt.Errorf("-src is required"))
	}
	policy := changePolicy(strings.ToLower(*onChange))
	switch policy {
	case polSnapshot, polSkip, polFail:
	default:
		fail(fmt.Errorf("invalid -on-change: %q (use snapshot|skip|fail)", *onChange))
	}
	mode := prescanMode(strings.ToLower(*prescan))
	switch mode {
	case preNone, preFast, preStable:
	default:
		fail(fmt.Errorf("invalid -prescan: %q (use none|fast|stable)", *prescan))
	}
	stabilityOn := strings.ToLower(*stability) != "off"

	// parse interval
	var progEvery time.Duration
	if *progress != "" {
		d, err := time.ParseDuration(*progress)
		if err != nil {
			fail(fmt.Errorf("invalid -progress: %w", err))
		}
		progEvery = d
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// output target (supports strftime-like placeholders)
	var dst io.Writer = os.Stdout
	var outFile *os.File
	var outPath string
	var pruneGlob string
	if *out != "-" {
		expanded := strftimeLike(*out, time.Now())
		pruneGlob = templateToGlob(*out) // auto-derive pruning glob
		if dir := filepath.Dir(expanded); dir != "." && dir != "" {
			if mkerr := os.MkdirAll(dir, 0o755); mkerr != nil {
				fail(mkerr)
			}
		}
		f, err := os.Create(expanded)
		if err != nil {
			fail(err)
		}
		outFile = f
		outPath = expanded
		dst = f
		defer outFile.Close()
	}

	gz, err := gzip.NewWriterLevel(dst, gzip.DefaultCompression)
	if err != nil {
		removePartial(outPath)
		fail(err)
	}
	defer gz.Close()

	tw := tar.NewWriter(gz)
	defer tw.Close()

	srcAbs, err := filepath.Abs(*src)
	if err != nil {
		removePartial(outPath)
		fail(err)
	}

	// prescan totals (for % + ETA)
	var totalPlanned int64
	var plannedFiles int64
	if mode != preNone {
		startPS := time.Now()
		effectiveMode := mode
		if !stabilityOn && mode == preStable {
			effectiveMode = preFast
		}
		totalPlanned, plannedFiles, err = planTotals(ctx, srcAbs, effectiveMode, time.Duration(*stabMS)*time.Millisecond, *retries, stabilityOn)
		if err != nil {
			fail(fmt.Errorf("prescan failed: %w", err))
		}
		note := ""
		if effectiveMode == preFast {
			note = " — note: totals are estimates"
		}
		log.Printf("prescan: files=%d bytes=%s (mode=%s, elapsed=%s)%s",
			plannedFiles, humanBytes(totalPlanned), effectiveMode, time.Since(startPS).Truncate(time.Millisecond), note)
	}

	// overall progress
	var totalDone int64
	var filesDone int64
	overallStart := time.Now()

	walkErr := filepath.WalkDir(srcAbs, func(pathFS string, d os.DirEntry, inErr error) error {
		if inErr != nil {
			return inErr
		}
		select {
		case <-ctx.Done():
			return errAborted
		default:
		}

		rel, err := filepath.Rel(srcAbs, pathFS)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if name == "." {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		// directory
		if info.IsDir() {
			h, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return err
			}
			if !strings.HasSuffix(name, "/") {
				name += "/"
			}
			h.Name = name
			return tw.WriteHeader(h)
		}

		// symlink
		if d.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(pathFS)
			if err != nil {
				return err
			}
			h, err := tar.FileInfoHeader(info, target)
			if err != nil {
				return err
			}
			h.Name = name
			return tw.WriteHeader(h)
		}

		// regular file
		if d.Type().IsRegular() {
			var snap os.FileInfo
			var ok bool

			if stabilityOn {
				// anti-flap snapshot (ctx-aware, infinite retries by default)
				snap, ok, err = stableSnapshot(ctx, pathFS, time.Duration(*stabMS)*time.Millisecond, *retries)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return errAborted
					}
					return err
				}
				if !ok {
					msg := fmt.Sprintf("unstable file: %s (could not stabilize)", name)
					switch policy {
					case polSkip, polSnapshot:
						log.Printf("warn: %s — skipping", msg)
						return nil
					case polFail:
						return errors.New(msg)
					}
				}
			} else {
				// stability off: single stat, copy as-is (with safety to keep tar valid)
				snap, err = os.Stat(pathFS)
				if err != nil {
					return err
				}
				ok = true
			}

			in, err := os.Open(pathFS)
			if err != nil {
				return err
			}
			defer in.Close()

			if stabilityOn {
				// ensure unchanged vs snapshot before copying
				cur, err := in.Stat()
				if err != nil {
					return err
				}
				if cur.Size() != snap.Size() || !cur.ModTime().Equal(snap.ModTime()) {
					if policy == polFail {
						return fmt.Errorf("file changed before copy: %s", name)
					}
					if policy == polSnapshot {
						in.Close()
						snap2, ok2, err2 := stableSnapshot(ctx, pathFS, time.Duration(*stabMS)*time.Millisecond, *retries)
						if err2 != nil {
							if errors.Is(err2, context.Canceled) {
								return errAborted
							}
							return err2
						}
						if !ok2 {
							log.Printf("warn: unstable file: %s — skipping", name)
							return nil
						}
						in, err = os.Open(pathFS)
						if err != nil {
							return err
						}
						defer in.Close()
						cur, err = in.Stat()
						if err != nil {
							return err
						}
						if cur.Size() != snap2.Size() || !cur.ModTime().Equal(snap2.ModTime()) {
							log.Printf("warn: still changing: %s — skipping", name)
							return nil
						}
						snap = snap2
					} else { // polSkip
						log.Printf("warn: file changed before copy: %s — skipping", name)
						return nil
					}
				}
			} // else: single stat behavior already set

			// header from snapshot or single stat
			h, err := tar.FileInfoHeader(snap, "")
			if err != nil {
				return err
			}
			h.Name = name
			if err := tw.WriteHeader(h); err != nil {
				return err
			}

			// per-file logging
			fileSize := h.Size
			fileStart := time.Now()
			if *verbose {
				log.Printf("start: %s size=%s", name, humanBytes(fileSize))
			} else {
				log.Printf("start: %s size=%s", name, humanBytes(fileSize))
			}

			wrote, err := copyExactWithProgress(
				ctx, tw, in, fileSize,
				progEvery, *inline,
				func(done int64, sinceStart time.Duration, speedBps float64) {
					if progEvery > 0 && fileSize > 0 {
						// overall % (clamped) + ETA (only before 100%)
						totalNow := atomic.LoadInt64(&totalDone) + done
						pct := ""
						eta := ""
						if totalPlanned > 0 {
							p := float64(totalNow) / float64(totalPlanned) * 100.0
							if p > 100 {
								p = 100
							}
							pct = fmt.Sprintf(" total=%.1f%%", p)
							if totalNow < totalPlanned {
								elapsed := time.Since(overallStart).Seconds()
								if elapsed > 0 {
									speedTotal := float64(totalNow) / elapsed
									if speedTotal > 0 {
										remain := float64(totalPlanned-totalNow) / speedTotal
										eta = fmt.Sprintf(" ETA=%s", (time.Duration(remain * float64(time.Second))).Truncate(time.Second))
									}
								}
							}
						}
						msg := fmt.Sprintf("progress: %s %s/%s (%.1f%%) elapsed=%s speed=%s%s%s",
							name,
							humanBytes(done), humanBytes(fileSize),
							float64(done)/float64(fileSize)*100.0,
							sinceStart.Truncate(time.Second),
							humanBytes(int64(speedBps))+"/s",
							pct, eta,
						)
						if *inline {
							fmt.Fprintf(os.Stderr, "\r%s", msg)
						} else {
							log.Println(msg)
						}
					}
				},
				policy, name,
			)

			// ensure newline after inline updates
			if *inline && progEvery > 0 {
				fmt.Fprintln(os.Stderr)
			}

			if err != nil {
				return err
			}

			elapsed := time.Since(fileStart)
			atomic.AddInt64(&totalDone, wrote)
			atomic.AddInt64(&filesDone, 1)

			avg := float64(wrote) / (elapsed.Seconds() + 1e-9)
			overallPct := ""
			if totalPlanned > 0 {
				prog := float64(atomic.LoadInt64(&totalDone)) / float64(totalPlanned) * 100.0
				if prog > 100 {
					prog = 100
				}
				overallPct = fmt.Sprintf(" total=%.1f%%", prog)
			}
			log.Printf("done:   %s bytes=%s elapsed=%s avg=%s%s",
				name, humanBytes(wrote), elapsed.Truncate(time.Millisecond), humanBytes(int64(avg))+"/s", overallPct)

			return nil
		}

		return nil // skip other types
	})

	// finalize writers & file
	if cerr := tw.Close(); walkErr == nil && cerr != nil {
		walkErr = cerr
	}
	if cerr := gz.Close(); walkErr == nil && cerr != nil {
		walkErr = cerr
	}
	if outFile != nil {
		if cerr := outFile.Sync(); walkErr == nil && cerr != nil {
			walkErr = cerr
		}
		if cerr := outFile.Close(); walkErr == nil && cerr != nil {
			walkErr = cerr
		}
	}

	elapsedAll := time.Since(overallStart)

	switch {
	case walkErr == nil:
		summary := fmt.Sprintf("done: files=%d, bytes=%s, elapsed=%s",
			atomic.LoadInt64(&filesDone), humanBytes(atomic.LoadInt64(&totalDone)), elapsedAll.Truncate(time.Millisecond))
		if totalPlanned > 0 {
			prog := float64(atomic.LoadInt64(&totalDone)) / float64(totalPlanned) * 100.0
			if prog > 100 {
				prog = 100
			}
			summary += fmt.Sprintf(" (planned=%s, %.1f%%)", humanBytes(totalPlanned), prog)
		}
		log.Println(summary)
	default:
		removePartial(outPath)
		if errors.Is(walkErr, errAborted) || errors.Is(walkErr, context.Canceled) {
			if outPath != "" {
				log.Printf("aborted: removed partial archive: %s (elapsed=%s)", outPath, elapsedAll.Truncate(time.Millisecond))
			} else {
				log.Printf("aborted (elapsed=%s)", elapsedAll.Truncate(time.Millisecond))
			}
			os.Exit(130)
		}
		fail(walkErr)
	}

	// simple prune: keep EXACTLY newest N by mtime, always include current
	if *keep > 0 && outPath != "" {
		pattern := pruneGlob
		if pattern == "" {
			pattern = outPath
		}
		deleted, skipped, perr := pruneKeepExactlyNewestByMTime(pattern, *keep, outPath)
		if perr != nil {
			log.Printf("prune: error: %v", perr)
		} else {
			log.Printf("prune: kept=%d, deleted=%d, skipped(current)=%d, pattern=%q", *keep, deleted, skipped, pattern)
		}
	}
}

// copyExactWithProgress copies exactly want bytes; pads zeros if the file shrinks (unless policy==fail).
func copyExactWithProgress(
	ctx context.Context,
	dst io.Writer,
	src io.Reader,
	want int64,
	interval time.Duration,
	inline bool,
	onTick func(done int64, sinceStart time.Duration, speedBps float64),
	policy changePolicy,
	name string,
) (int64, error) {

	if want == 0 {
		return 0, nil
	}

	buf := make([]byte, 1<<20) // 1 MiB
	var done int64
	var last int64
	start := time.Now()

	var ticker *time.Ticker
	if interval > 0 {
		ticker = time.NewTicker(interval) // IMPORTANT: do not copy ticker
		defer ticker.Stop()
	}

	for done < want {
		select {
		case <-ctx.Done():
			return done, errAborted
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
				return done, ew
			}
			if nw != nr {
				return done, io.ErrShortWrite
			}
			done += int64(nw)
		}

		// interim progress
		if ticker != nil {
			select {
			case <-ticker.C:
				elapsed := time.Since(start)
				sec := interval.Seconds()
				if sec <= 0 {
					sec = 1
				}
				speed := float64(done-last) / sec
				last = done
				if onTick != nil {
					onTick(done, elapsed, speed)
				}
			default:
			}
		}

		if er != nil {
			if errors.Is(er, io.EOF) {
				remain := want - done
				if remain > 0 {
					if policy == polFail {
						return done, fmt.Errorf("short read for %q: file changed during backup", name)
					}
					if _, perr := io.CopyN(dst, zeroReader{}, remain); perr != nil {
						return done, fmt.Errorf("padding failed for %q: %w", name, perr)
					}
				}
				return want, nil
			}
			return done, er
		}
	}

	return done, nil
}

// planTotals prescans to estimate or determine total bytes and file count.
func planTotals(ctx context.Context, root string, mode prescanMode, delay time.Duration, retries int, stabilityOn bool) (int64, int64, error) {
	var total int64
	var count int64
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, inErr error) error {
		if inErr != nil {
			return inErr
		}
		select {
		case <-ctx.Done():
			return errAborted
		default:
		}
		if d.IsDir() {
			return nil
		}
		if d.Type().IsRegular() {
			switch mode {
			case preFast:
				info, err := d.Info()
				if err != nil {
					return err
				}
				total += info.Size()
				count++
			case preStable:
				if !stabilityOn {
					// treat as fast if stability is disabled
					info, err := d.Info()
					if err != nil {
						return err
					}
					total += info.Size()
					count++
					return nil
				}
				snap, ok, err := stableSnapshot(ctx, p, delay, retries)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return errAborted
					}
					return err
				}
				if ok {
					total += snap.Size()
					count++
				}
			case preNone:
			}
		}
		return nil
	})
	return total, count, err
}

// stableSnapshot waits for consistent size+mtime across two stats with a delay.
// If retries < 0, it retries indefinitely until stable or context is canceled.
func stableSnapshot(ctx context.Context, path string, delay time.Duration, retries int) (os.FileInfo, bool, error) {
	if delay <= 0 {
		delay = 50 * time.Millisecond
	}
	prev, err := os.Stat(path)
	if err != nil {
		return nil, false, err
	}
	if retries < 0 {
		for {
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-time.After(delay):
			}
			cur, err := os.Stat(path)
			if err != nil {
				return nil, false, err
			}
			if cur.Size() == prev.Size() && cur.ModTime().Equal(prev.ModTime()) {
				return cur, true, nil
			}
			prev = cur
		}
	}
	for i := 0; i <= retries; i++ {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(delay):
		}
		cur, err := os.Stat(path)
		if err != nil {
			return nil, false, err
		}
		if cur.Size() == prev.Size() && cur.ModTime().Equal(prev.ModTime()) {
			return cur, true, nil
		}
		prev = cur
	}
	return nil, false, nil
}

// strftimeLike formats a path template with strftime-style tokens using local time.
// Supported: %Y %m %d %H %M %S %y %F %T %z %:z and %%.
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

// templateToGlob converts an -out template (with % tokens) into a glob pattern.
// Every %... token becomes '*', and '%%' also becomes '*'.
func templateToGlob(tpl string) string {
	var b strings.Builder
	runes := []rune(tpl)
	for i := 0; i < len(runes); i++ {
		if runes[i] == '%' {
			b.WriteRune('*')
			// consume following token rune(s) loosely
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

func removePartial(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

func fail(err error) {
	log.Printf("Error: %v", err)
	os.Exit(1)
}

// pruneKeepExactlyNewestByMTime keeps exactly 'keep' newest files by mtime,
// ensuring the current file is included; deletes the rest.
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

	// Newest first
	sort.Slice(items, func(i, j int) bool { return items[i].mt.After(items[j].mt) })

	if len(items) <= keep {
		return 0, 0, nil
	}

	// Build keep set (first N)
	keepSet := make(map[int]struct{}, keep)
	for i := 0; i < keep && i < len(items); i++ {
		keepSet[i] = struct{}{}
	}

	// Ensure 'current' is kept
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
				// Replace the oldest keeper (largest index in keepSet)
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

	// Delete the rest
	for i, it := range items {
		if _, keep := keepSet[i]; keep {
			continue
		}
		// safety (shouldn't trigger now)
		if current != "" {
			absCur, _ := filepath.Abs(current)
			absPath, _ := filepath.Abs(it.path)
			if absCur == absPath {
				skippedCurrent++
				continue
			}
		}
		log.Printf("prune: remove %s", it.path)
		if remErr := os.Remove(it.path); remErr != nil {
			err = remErr
			log.Printf("prune: failed to remove %s: %v", it.path, remErr)
			continue
		}
		deleted++
	}
	return deleted, skippedCurrent, err
}
