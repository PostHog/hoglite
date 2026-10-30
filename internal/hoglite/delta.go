package hoglite

// Page deltas (manifest format 2).
//
// A checkpoint of a chain generation uploads only the pages that changed
// since the previous checkpoint, not the whole database. The state at the
// start of a generation is the base image with every delta of
// Manifest.Deltas applied in order. The segments of the generation then
// replay on top of it, as before.
//
// Source of a delta. Between two checkpoints SQLite writes the database
// file only in the checkpoint (WAL mode, no autocheckpoint). So the pages
// that change are exactly the pages of the committed WAL frames, and the
// new size is the database size of the last commit frame. The checkpoint
// lists those page numbers from the WAL, lets SQLite copy the WAL into
// the database file, and then reads the page contents from the database
// file. The delta thus holds the bytes that SQLite itself wrote, and
// applying it to the previous state gives the same file, byte for byte.
// Two checks guard the assumptions: SQLite must report exactly the frame
// count we parsed, and the file must have the size the last commit frame
// names. If either check fails, the checkpoint uploads a full base.
//
// Raw layout (all integers big-endian), stored as one zstd frame:
//
//	off  size
//	0    8   magic "HLPGDLT1"
//	8    4   page size
//	12   4   number of page records n
//	16   4   database size in pages after the delta
//	20   4   position of the delta in Manifest.Deltas
//	24   8   database file size in bytes before the delta
//	32   2   length L of the base key
//	34   L   base key
//	then n records: 4-byte page number + one page, page numbers strictly
//	ascending, each at most the new database size.
//
// The header binds a delta to its base, its position and the file size it
// applies to, so a delta out of order or on the wrong base fails loudly.
// The zstd frame checksum covers the content.
//
// Compatibility: builds that read format 1 refuse format 2 (the format
// gate). Builds before compression ignore Format and Deltas. They read the
// base and the segments raw, and both are zstd frames in a chain
// generation, so they refuse before their claim CAS, as for format 1.
// Their sweeper skips ckpt/ keys (see ckptKey).

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	deltaMagic     = "HLPGDLT1"
	deltaFixedHdr  = 34
	maxDeltaKeyLen = 1024
	// stored deltas download this many at a time during recovery, and at
	// most this many sit on local disk (see applyChain). Each GET is one
	// round trip, so a chain of 32 costs 4 round trips, not 32.
	deltaFetchParallel = 8
)

// maxChainLen is the longest chain a reader accepts. Writers never
// publish a longer one: the checkpoint rebases at min(DeltaMaxChain,
// maxChainLen), and NewManager clamps DeltaMaxChain to it. A variable
// only so that tests can lower it.
var maxChainLen = 1024

// maxDeltaRawBytes is the writer and reader limit of one raw delta: every
// page of the largest base plus its page number, plus the header.
const maxDeltaRawBytes = maxBaseRawBytes + maxBaseRawBytes/128 + 1<<16

type deltaHeader struct {
	pageSize  uint32
	nRecords  uint32
	dbPages   uint32
	index     uint32
	prevBytes uint64
	baseKey   string
}

func (h *deltaHeader) marshal() []byte {
	b := make([]byte, deltaFixedHdr+len(h.baseKey))
	copy(b, deltaMagic)
	binary.BigEndian.PutUint32(b[8:], h.pageSize)
	binary.BigEndian.PutUint32(b[12:], h.nRecords)
	binary.BigEndian.PutUint32(b[16:], h.dbPages)
	binary.BigEndian.PutUint32(b[20:], h.index)
	binary.BigEndian.PutUint64(b[24:], h.prevBytes)
	binary.BigEndian.PutUint16(b[32:], uint16(len(h.baseKey)))
	copy(b[deltaFixedHdr:], h.baseKey)
	return b
}

// rawSize is the exact raw length of a delta with this header.
func (h *deltaHeader) rawSize() int64 {
	return int64(deltaFixedHdr+len(h.baseKey)) + int64(h.nRecords)*(4+int64(h.pageSize))
}

func validPageSize(ps int64) bool {
	return ps >= 512 && ps <= 65536 && ps&(ps-1) == 0
}

// walDelta describes the committed part of a local WAL: the pages it
// changes and the database size after its last commit.
type walDelta struct {
	frames  int64    // committed frames
	pages   []uint32 // distinct page numbers <= dbPages, ascending
	dbPages uint32   // database size in pages after the last commit
}

// scanWALPages reads the frame headers of the committed WAL prefix
// [0, commitEnd). The caller has verified the checksum chain up to
// commitEnd (walParser), so only headers are read: memory is one entry per
// distinct page, never the WAL itself. Frames after commitEnd (a rolled
// back spill) are not part of the database and are ignored.
func scanWALPages(wal io.ReaderAt, commitEnd, pageSize int64) (*walDelta, error) {
	if commitEnd == 0 {
		return &walDelta{}, nil
	}
	var hdr [walHdrSize]byte
	if _, err := wal.ReadAt(hdr[:], 0); err != nil {
		return nil, fmt.Errorf("wal header: %w", err)
	}
	if ps := int64(binary.BigEndian.Uint32(hdr[8:12])); ps != pageSize {
		return nil, fmt.Errorf("wal page size %d, database page size %d", ps, pageSize)
	}
	frameSize := walFrmHdr + pageSize
	if commitEnd < walHdrSize+frameSize || (commitEnd-walHdrSize)%frameSize != 0 {
		return nil, fmt.Errorf("commit end %d is not on a frame boundary", commitEnd)
	}
	d := &walDelta{frames: (commitEnd - walHdrSize) / frameSize}
	seen := make(map[uint32]struct{})
	var fh [walFrmHdr]byte
	for i := int64(0); i < d.frames; i++ {
		if _, err := wal.ReadAt(fh[:], walHdrSize+i*frameSize); err != nil {
			return nil, fmt.Errorf("wal frame %d: %w", i, err)
		}
		pg := binary.BigEndian.Uint32(fh[0:4])
		if pg == 0 {
			return nil, fmt.Errorf("wal frame %d: page 0", i)
		}
		seen[pg] = struct{}{}
		if i == d.frames-1 {
			d.dbPages = binary.BigEndian.Uint32(fh[4:8])
			if d.dbPages == 0 {
				return nil, errors.New("last committed frame is not a commit frame")
			}
		}
	}
	// a page above the final size was truncated away by a later commit:
	// SQLite's checkpoint skips it too
	for pg := range seen {
		if pg <= d.dbPages {
			d.pages = append(d.pages, pg)
		}
	}
	sort.Slice(d.pages, func(i, j int) bool { return d.pages[i] < d.pages[j] })
	return d, nil
}

// deltaReader produces the raw delta: the header, then each listed page
// read from the database file. It holds one page in memory.
type deltaReader struct {
	db    io.ReaderAt
	ps    int64
	pages []uint32
	cur   []byte // unread rest of the current unit
	buf   []byte
	prog  func()
}

func (r *deltaReader) Read(p []byte) (int, error) {
	for len(r.cur) == 0 {
		if len(r.pages) == 0 {
			return 0, io.EOF
		}
		pg := r.pages[0]
		r.pages = r.pages[1:]
		binary.BigEndian.PutUint32(r.buf[:4], pg)
		if _, err := r.db.ReadAt(r.buf[4:], int64(pg-1)*r.ps); err != nil {
			return 0, fmt.Errorf("read page %d: %w", pg, err)
		}
		r.cur = r.buf
		if r.prog != nil {
			r.prog()
		}
	}
	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

// writeDelta compresses the delta for (h, pages) into dst as one zstd
// frame and returns the raw and stored sizes. db must already hold the
// checkpointed pages. prog is called per page (watchdog progress).
func writeDelta(dst io.Writer, db io.ReaderAt, h deltaHeader, pages []uint32, prog func()) (raw, stored int64, err error) {
	h.nRecords = uint32(len(pages))
	ps := int64(h.pageSize)
	src := &deltaReader{db: db, ps: ps, pages: pages, cur: h.marshal(), buf: make([]byte, 4+ps), prog: prog}
	raw = h.rawSize()
	stored, err = compressStream(dst, src, raw, maxDeltaRawBytes)
	return raw, stored, err
}

// deltaExpect is what recovery knows about a delta before it reads it.
type deltaExpect struct {
	baseKey  string
	index    uint32
	pageSize int64 // 0: not known yet (taken from the first delta)
}

// errBadDelta marks a delta that decodes but does not fit the chain. It
// wraps ErrCorruptObject: an immutable object that is wrong is corruption.
func errBadDelta(format string, args ...any) error {
	return fmt.Errorf("%w: delta: %s", ErrCorruptObject, fmt.Sprintf(format, args...))
}

// deltaApplier is an io.Writer that parses a raw delta stream and writes
// its pages into the database file as they arrive. It buffers at most one
// record. finish checks completeness and sets the final file size.
type deltaApplier struct {
	db    *os.File
	exp   deltaExpect
	h     deltaHeader
	buf   []byte
	need  int
	state int
	seen  uint32
	last  uint32
	err   error
}

const (
	daFixed = iota
	daKey
	daRecord
	daDone
)

func newDeltaApplier(db *os.File, exp deltaExpect) *deltaApplier {
	return &deltaApplier{db: db, exp: exp, need: deltaFixedHdr}
}

func (a *deltaApplier) Write(p []byte) (int, error) {
	if a.err != nil {
		return 0, a.err
	}
	total := len(p)
	for len(p) > 0 {
		if a.state == daDone {
			a.err = errBadDelta("data after the last record")
			return 0, a.err
		}
		k := a.need - len(a.buf)
		if k > len(p) {
			k = len(p)
		}
		a.buf = append(a.buf, p[:k]...)
		p = p[k:]
		if len(a.buf) < a.need {
			break
		}
		if err := a.consume(); err != nil {
			a.err = err
			return 0, err
		}
		a.buf = a.buf[:0]
	}
	return total, nil
}

func (a *deltaApplier) consume() error {
	b := a.buf
	switch a.state {
	case daFixed:
		if string(b[:8]) != deltaMagic {
			return errBadDelta("bad magic")
		}
		a.h.pageSize = binary.BigEndian.Uint32(b[8:])
		a.h.nRecords = binary.BigEndian.Uint32(b[12:])
		a.h.dbPages = binary.BigEndian.Uint32(b[16:])
		a.h.index = binary.BigEndian.Uint32(b[20:])
		a.h.prevBytes = binary.BigEndian.Uint64(b[24:])
		kl := int(binary.BigEndian.Uint16(b[32:]))
		ps := int64(a.h.pageSize)
		if !validPageSize(ps) {
			return errBadDelta("page size %d", ps)
		}
		if a.exp.pageSize != 0 && ps != a.exp.pageSize {
			return errBadDelta("page size %d, database has %d", ps, a.exp.pageSize)
		}
		if a.h.index != a.exp.index {
			return errBadDelta("position %d, manifest position %d", a.h.index, a.exp.index)
		}
		if kl == 0 || kl > maxDeltaKeyLen {
			return errBadDelta("base key length %d", kl)
		}
		if a.h.nRecords > a.h.dbPages {
			return errBadDelta("%d records for %d pages", a.h.nRecords, a.h.dbPages)
		}
		a.state, a.need = daKey, kl
	case daKey:
		if string(b) != a.exp.baseKey {
			return errBadDelta("made for base %q, chain base is %q", b, a.exp.baseKey)
		}
		fi, err := a.db.Stat()
		if err != nil {
			return err
		}
		if uint64(fi.Size()) != a.h.prevBytes {
			return errBadDelta("applies to a %d-byte file, the chain gives %d bytes", a.h.prevBytes, fi.Size())
		}
		if a.h.nRecords == 0 {
			a.state = daDone
		} else {
			a.state, a.need = daRecord, 4+int(a.h.pageSize)
			a.buf = make([]byte, 0, a.need)
		}
	case daRecord:
		pg := binary.BigEndian.Uint32(b[:4])
		if pg == 0 || pg <= a.last || pg > a.h.dbPages {
			return errBadDelta("page %d after page %d, database size %d pages", pg, a.last, a.h.dbPages)
		}
		if _, err := a.db.WriteAt(b[4:], int64(pg-1)*int64(a.h.pageSize)); err != nil {
			return err
		}
		a.last = pg
		a.seen++
		if a.seen == a.h.nRecords {
			a.state = daDone
		}
	}
	return nil
}

// finish checks that the whole delta arrived and sets the database size
// to the size after the delta (SQLite's checkpoint truncates the same way).
func (a *deltaApplier) finish() error {
	if a.err != nil {
		return a.err
	}
	if a.state != daDone || len(a.buf) != 0 {
		return errBadDelta("truncated: %d of %d records", a.seen, a.h.nRecords)
	}
	return a.db.Truncate(int64(a.h.dbPages) * int64(a.h.pageSize))
}

// applyDelta decodes one stored delta (one zstd frame) from r and applies
// it to db. It returns the page size of the delta.
func applyDelta(db *os.File, r io.Reader, exp deltaExpect) (int64, error) {
	a := newDeltaApplier(db, exp)
	if _, err := decompressStream(a, r, maxDeltaRawBytes); err != nil {
		if a.err != nil {
			return 0, a.err
		}
		return 0, err
	}
	if err := a.finish(); err != nil {
		return 0, err
	}
	return int64(a.h.pageSize), nil
}

// sqlitePageSize reads the page size from a database header. It returns 0
// for a file too short to have one.
func sqlitePageSize(f *os.File) (int64, error) {
	var h [100]byte
	if _, err := f.ReadAt(h[:], 0); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, err
	}
	ps := int64(binary.BigEndian.Uint16(h[16:18]))
	if ps == 1 {
		ps = 65536
	}
	return ps, nil
}

// applyChain applies every delta of mf, in order, to the base image in db.
// Stored deltas download into temp files in dir through a window: at most
// deltaFetchParallel deltas are in flight or on disk ahead of the one
// being applied. A slot frees only when its delta is applied and removed,
// and slots are taken in chain order, so the next delta to apply always
// has a slot (no deadlock) and local disk holds at most the window. One
// decoder runs per materialization. A missing delta means the chain moved
// on (errStaleGen), unless the current manifest still names it: that is
// data loss, and a retry cannot help.
func applyChain(ctx context.Context, store Backend, tid string, mf *Manifest, dir string, db *os.File) error {
	n := len(mf.Deltas)
	if n == 0 {
		return nil
	}
	if n > maxChainLen {
		return fmt.Errorf("%w: chain of %d deltas, the limit is %d", ErrCorruptObject, n, maxChainLen)
	}
	ps, err := sqlitePageSize(db)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	type fetched struct {
		path string
		size int64
		err  error
	}
	res := make([]chan fetched, n)
	for i := range res {
		res[i] = make(chan fetched, 1)
	}
	deltaPath := func(i int) string { return filepath.Join(dir, fmt.Sprintf("delta-%04d.zst", i)) }
	window := make(chan struct{}, deltaFetchParallel)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // dispatcher: one slot per delta, in chain order
		defer wg.Done()
		for i, d := range mf.Deltas {
			select {
			case window <- struct{}{}:
			case <-ctx.Done():
				return
			}
			wg.Add(1)
			go func(i int, key string) {
				defer wg.Done()
				f, err := os.Create(deltaPath(i))
				if err != nil {
					res[i] <- fetched{err: err}
					return
				}
				cw := &countingWriter{w: f}
				err = store.GetObjectTo(ctx, key, cw)
				if cerr := f.Close(); err == nil {
					err = cerr
				}
				res[i] <- fetched{path: f.Name(), size: cw.n, err: err}
			}(i, d.Key)
		}
	}()
	defer func() {
		cancel()
		wg.Wait()
		for i := range mf.Deltas {
			os.Remove(deltaPath(i))
		}
	}()
	for i, d := range mf.Deltas {
		var r fetched
		select {
		case r = <-res[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
		if r.err != nil {
			if errors.Is(r.err, ErrNotFound) {
				if cur, _, err := store.GetManifest(ctx, tid); err == nil && cur != nil && manifestNames(cur, d.Key) {
					return fmt.Errorf("%w: delta %s of the current manifest is missing", ErrCorruptObject, d.Key)
				}
				return errStaleGen
			}
			return fmt.Errorf("fetch delta %s: %w", d.Key, r.err)
		}
		if r.size != d.Size {
			return fmt.Errorf("fetch delta %s: %w: stored size %d, manifest says %d", d.Key, ErrCorruptObject, r.size, d.Size)
		}
		f, err := os.Open(r.path)
		if err != nil {
			return err
		}
		got, err := applyDelta(db, f, deltaExpect{baseKey: mf.BaseKey, index: uint32(i), pageSize: ps})
		f.Close()
		os.Remove(r.path)
		<-window // applied and removed: the next delta may download
		if err != nil {
			return fmt.Errorf("fetch delta %s: %w", d.Key, err)
		}
		ps = got
	}
	return nil
}
