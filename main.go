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
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/klauspost/pgzip"
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

var bufPool sync.Pool

func main() {
	// --- Flags ---
	src := flag.String("src", "", "Source directory")
	out := flag.String("out", "-", "Output file path or '-' for stdout")

	// Flapping prevention
	stability := flag.String("stability", "on", "Flapping prevention: on|off")
	stabMS := flag.Int("stability-ms", 200, "Stability window per check (ms)")
	retries := flag.Int("retries", -1, "Max retries to obtain a stable snapshot (-1 = infinite)")
	onChange := flag.String("on-change", "snapshot", "Policy for changing files: snapshot|skip|fail")

	// Progress / totals
	progress := flag.String("progress", "10s", "Per-file progress interval (e.g. 5s, 30s; 0 to disable interim updates)")
	inline := flag.Bool("inline", false, "Inline progress on a single line (carriage return)")
	prescan := flag.String("prescan", "none", "Prescan mode: none|fast|stable")

	// Pruning
	keep := flag.Int("keep", 0, "Keep newest N archives by mtime (0 disables pruning)")

	// Compression & memory (pgzip on; RAM-safe defaults)
	usePgzip := flag.Bool("pgzip", true, "Use pgzip for parallel compression")
	gzipLevel := flag.Int("gzip-level", 3, "GZIP compression level (1-9)")
	bufSize := flag.Int("bufsize", 4<<20, "I/O buffer size (64KiB..32MiB)")
	memCap := flag.String("mem-cap", "auto", "Memory cap (e.g. 128MiB, 256MB, bytes, or 'auto')")
	pgBlock := flag.String("pgzip-block", "auto", "pgzip block size (e.g. 256KiB, 1MiB, or 'auto')")
	pgBlocks := flag.Int("pgzip-blocks", 0, "pgzip in-flight blocks (0=auto)")

	// Durability
	fsyncFlag := flag.String("fsync", "on", "fsync output at end: on|off")

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

	// --- Memory & buffer tuning ---
	tune := deriveTuning(*memCap, *pgBlock, *pgBlocks, *bufSize)
	// best-effort: limit Go heap a bit below the container cap
	if tune.hardCap > 0 {
		debug.SetMemoryLimit(tune.hardCap)
	}
	bsize := clamp(tune.bufSize, 64<<10, 32<<20)
	bufPool.New = func() any { return make([]byte, bsize) }
	log.Printf("tuning: mem-cap=%s buf=%s gzip-level=%d pgzip: block=%s blocks=%d",
		humanBytes(tune.hardCap), humanBytes(int64(bsize)), clamp(*gzipLevel, 1, 9),
		humanBytes(int64(tune.pgBlockSize)), tune.pgBlocks)

	// --- Progress interval ---
	var progEvery time.Duration
	if *progress != "" {
		d, err := time.ParseDuration(*progress)
		if err != nil {
			fail(fmt.Errorf("invalid -progress: %w", err))
		}
		progEvery = d
	}

	// --- Signals / context ---
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Output target ---
	var dst io.Writer = os.Stdout
	var outFile *os.File
	var outPath string
	var pruneGlob string
	if *out != "-" {
		expanded := strftimeLike(*out, time.Now())
		pruneGlob = templateToGlob(*out) // for -keep
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

	// --- compressor (always .gz), pgzip (parallel) or stdlib gzip ---
	zw, err := newGzipWriter(dst, *usePgzip, clamp(*gzipLevel, 1, 9), tune.pgBlockSize, tune.pgBlocks)
	if err != nil {
		removePartial(outPath)
		fail(err)
	}
	defer zw.Close()

	tw := tar.NewWriter(zw)
	defer tw.Close()

	srcAbs, err := filepath.Abs(*src)
	if err != nil {
		removePartial(outPath)
		fail(err)
	}

	// --- Prescan totals (for overall %) ---
	var totalPlanned int64
	var plannedFiles int64
	if mode != preNone {
		startPS := time.Now()
		effective := mode
		if !stabilityOn && mode == preStable {
			effective = preFast
		}
		totalPlanned, plannedFiles, err = planTotals(ctx, srcAbs, effective, time.Duration(*stabMS)*time.Millisecond, *retries, stabilityOn)
		if err != nil {
			fail(fmt.Errorf("prescan failed: %w", err))
		}
		note := ""
		if effective == preFast {
			note = " — note: totals are estimates"
		}
		log.Printf("prescan: files=%d bytes=%s (mode=%s, elapsed=%s)%s",
			plannedFiles, humanBytes(totalPlanned), effective, time.Since(startPS).Truncate(time.Millisecond), note)
	}

	// --- Running totals ---
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

		// directories
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

		// symlinks
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

		// regular files
		if d.Type().IsRegular() {
			var snap os.FileInfo
			var ok bool

			if stabilityOn {
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

			// verify unchanged if stabilityOn
			if stabilityOn {
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
			}

			// write header
			h, err := tar.FileInfoHeader(snap, "")
			if err != nil {
				return err
			}
			h.Name = name
			if err := tw.WriteHeader(h); err != nil {
				return err
			}

			// per-file logging + copy
			fileSize := h.Size
			fileStart := time.Now()
			log.Printf("start: %s size=%s", name, humanBytes(fileSize))

			wrote, err := copyExactWithProgress(
				ctx, tw, in, fileSize,
				progEvery, *inline,
				func(done int64, sinceStart time.Duration, speedBps float64) {
					if progEvery > 0 && fileSize > 0 {
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

		return nil // everything else ignored
	})

	// --- finalize writers & file ---
	if cerr := tw.Close(); walkErr == nil && cerr != nil {
		walkErr = cerr
	}
	if cerr := zw.Close(); walkErr == nil && cerr != nil {
		walkErr = cerr
	}
	if outFile != nil {
		if strings.ToLower(*fsyncFlag) == "on" {
			if cerr := outFile.Sync(); walkErr == nil && cerr != nil {
				walkErr = cerr
			}
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

	// --- prune: keep exactly N newest; current is always kept ---
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

// --- gzip writer: pgzip (parallel) or stdlib gzip (single-threaded) ---
func newGzipWriter(dst io.Writer, usePGZIP bool, level int, blockSize int, blocks int) (io.WriteCloser, error) {
	if usePGZIP {
		pw, err := pgzip.NewWriterLevel(dst, level)
		if err != nil {
			return nil, err
		}
		// SetConcurrency controls per-block size & number of in-flight blocks.
		// This dominates RAM usage & throughput in a controlled way for K8s.
		if blockSize <= 0 {
			blockSize = 1 << 20 // default 1 MiB
		}
		if blocks < 2 {
			blocks = 2
		}
		pw.SetConcurrency(blockSize, blocks)
		return pw, nil
	}
	return gzip.NewWriterLevel(dst, level)
}

// --- copy with exact length & progress; pad zeros if file shrinks (unless policy==fail) ---
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

	bufAny := bufPool.Get()
	buf := bufAny.([]byte)
	defer bufPool.Put(buf)

	var done int64
	var last int64
	start := time.Now()

	var ticker *time.Ticker
	if interval > 0 {
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}

	for done < want {
		select {
		case <-ctx.Done():
			return done, errAborted
		default:
		}

		remain := want - done
		readBuf := buf
		if int64(len(readBuf)) > remain {
			readBuf = readBuf[:remain]
		}

		nr, er := src.Read(readBuf)
		if nr > 0 {
			nw, ew := dst.Write(readBuf[:nr])
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

// --- prescan totals ---
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

// --- stable snapshot (unchanged size+mtime across two stats) ---
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

// --- prune: keep exactly N newest (current always kept) ---
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

	// ensure current in keep set
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
				// drop the oldest kept index to make room
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

// --- utils ---
func deriveTuning(memCapStr string, pgBlockStr string, wantBlocks int, wantBuf int) (t memTuning) {
	const (
		minBuf   = 64 << 10
		maxBuf   = 32 << 20
		minBlock = 128 << 10
		maxBlock = 4 << 20
	)
	// cap bytes
	var capBytes int64
	switch strings.ToLower(strings.TrimSpace(memCapStr)) {
	case "auto", "":
		capBytes = cgroupV2MemLimit()
		// fallback if unknown
		if capBytes == 0 {
			capBytes = 256 << 20
		}
	default:
		v, err := parseSize(memCapStr)
		if err == nil && v > 0 {
			capBytes = v
		} else {
			capBytes = 256 << 20
		}
	}

	// buffer size target ~1/16 cap
	buf := wantBuf
	if buf <= 0 {
		buf = 4 << 20
	}
	targetBuf := int(capBytes / 16)
	if targetBuf < minBuf {
		targetBuf = minBuf
	}
	if targetBuf > maxBuf {
		targetBuf = maxBuf
	}
	if buf > targetBuf {
		buf = targetBuf
	}
	if buf < minBuf {
		buf = minBuf
	}

	// block size
	var block int
	if strings.ToLower(strings.TrimSpace(pgBlockStr)) != "auto" {
		if v, err := parseSize(pgBlockStr); err == nil && v > 0 {
			block = int(v)
		}
	}
	if block == 0 {
		// tighter under low caps
		switch {
		case capBytes <= 128<<20:
			block = 256 << 10
		case capBytes <= 256<<20:
			block = 512 << 10
		default:
			block = 1 << 20
		}
	}
	if block < minBlock {
		block = minBlock
	}
	if block > maxBlock {
		block = maxBlock
	}

	// blocks budget ~1/3 cap
	budget := capBytes / 3
	blocks := int(budget / int64(block))
	if blocks < 2 {
		blocks = 2
	}
	// also cap by CPUs to avoid over-parallelizing gzip kernel
	maxByCPU := runtime.GOMAXPROCS(0) * 4
	if blocks > maxByCPU {
		blocks = maxByCPU
	}
	// user override
	if wantBlocks > 0 && wantBlocks < blocks {
		blocks = wantBlocks
	}
	return memTuning{
		bufSize:     buf,
		pgBlockSize: block,
		pgBlocks:    blocks,
		hardCap:     capBytes,
	}
}

type memTuning struct {
	bufSize     int
	pgBlockSize int
	pgBlocks    int
	hardCap     int64
}

func cgroupV2MemLimit() int64 {
	// best effort
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err == nil {
		s := strings.TrimSpace(string(data))
		if s != "" && s != "max" {
			if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
				return v
			}
		}
	}
	return 0
}

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

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	type u struct {
		suf string
		mul int64
	}
	units := []u{
		{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30},
		{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000},
	}
	for _, x := range units {
		if strings.HasSuffix(s, x.suf) {
			v := strings.TrimSuffix(s, x.suf)
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				return 0, err
			}
			return int64(f * float64(x.mul)), nil
		}
	}
	return 0, fmt.Errorf("invalid size: %q", s)
}
