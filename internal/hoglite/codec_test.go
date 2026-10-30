package hoglite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// pageLike returns compressible bytes that resemble SQLite pages: repeated
// structure with some random content.
func pageLike(n int, seed int64) []byte {
	r := rand.New(rand.NewSource(seed))
	var b bytes.Buffer
	for b.Len() < n {
		fmt.Fprintf(&b, `{"path":"s3://bucket/main/events/data/%08x.parquet","rows":%d}`, r.Uint32(), r.Intn(1e6))
	}
	return b.Bytes()[:n]
}

func randomBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

// frameWindow returns the window a frame asks the decoder for.
func frameWindow(t *testing.T, b []byte) uint64 {
	t.Helper()
	var h zstd.Header
	if err := h.Decode(b); err != nil {
		t.Fatal(err)
	}
	if h.SingleSegment {
		return h.FrameContentSize
	}
	return h.WindowSize
}

func TestCodecBlobRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 100, 255, 256, 1023, 1024, 36 << 10, 1 << 20, codecWindow + 1, 3 << 20} {
		raw := pageLike(n, int64(n))
		enc := encodeBlob(raw)
		if string(enc[:4]) != zstdMagic {
			t.Fatalf("n=%d: not a frame", n)
		}
		// every frame we write fits the decoder window cap with room
		if w := frameWindow(t, enc); w > codecWindow {
			t.Fatalf("n=%d: window %d above %d", n, w, codecWindow)
		}
		got, err := decodeBlob(enc, maxSegmentRawBytes)
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("n=%d: round trip failed: %v", n, err)
		}
	}
}

// TestCodecNoSniffing: the generation format, not the content, decides
// the codec. A zstd reader refuses raw bytes, and a raw reader returns the
// bytes as they are, even when they start with the zstd magic.
func TestCodecNoSniffing(t *testing.T) {
	walHdr := []byte{0x37, 0x7f, 0x06, 0x82, 0, 0x2d, 0xe2, 0x18}
	sqlite := append([]byte("SQLite format 3\x00"), pageLike(1000, 6)...)
	for name, b := range map[string][]byte{"wal": walHdr, "sqlite": sqlite, "empty": nil} {
		if _, err := decodeBlob(b, maxSegmentRawBytes); !errors.Is(err, ErrCorruptObject) {
			t.Errorf("%s in a zstd generation: want ErrCorruptObject, got %v", name, err)
		}
	}
	ctx := context.Background()
	fs := NewFakeStore()
	collide := append([]byte(zstdMagic), randomBytes(4096, 3)...)
	fs.PutSegment(ctx, "fake/collide", collide)
	var out bytes.Buffer
	n, st, err := fetchObjectTo(ctx, fs, "fake/collide", &out, manifestFormatRaw, maxSegmentRawBytes)
	if err != nil || n != int64(len(collide)) || st != n || !bytes.Equal(out.Bytes(), collide) {
		t.Fatalf("raw generation must read bytes as they are: n=%d err=%v", n, err)
	}
}

func TestSegmentObjectForms(t *testing.T) {
	compressible := pageLike(36<<10, 1)
	noise := randomBytes(64<<10, 2)
	collide := append([]byte(zstdMagic), randomBytes(4096, 3)...)
	for _, raw := range [][]byte{compressible, noise, collide} {
		// raw generation: stored as they are, even the magic collision
		if s, err := segmentObject(raw, manifestFormatRaw); err != nil || !bytes.Equal(s, raw) {
			t.Fatalf("raw generation must store raw: %v", err)
		}
		// zstd generation: always one frame, even when it is larger
		s, err := segmentObject(raw, manifestFormatZstd)
		if err != nil || string(s[:4]) != zstdMagic {
			t.Fatalf("zstd generation must store a frame: %v", err)
		}
		got, err := decodeBlob(s, maxSegmentRawBytes)
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("round trip: %v", err)
		}
	}
	if s, _ := segmentObject(compressible, manifestFormatZstd); len(s) >= len(compressible)/2 {
		t.Fatalf("poor ratio: %d of %d", len(s), len(compressible))
	}
	// above the shared limit: refused, never published
	defer func(v int64) { maxSegmentRawBytes = v }(maxSegmentRawBytes)
	maxSegmentRawBytes = 1 << 20
	if _, err := segmentObject(make([]byte, 1<<20+1), manifestFormatZstd); !errors.Is(err, errSegmentTooLarge) {
		t.Fatalf("want errSegmentTooLarge, got %v", err)
	}
	if s, err := segmentObject(make([]byte, 1<<20), manifestFormatZstd); err != nil {
		t.Fatal(err)
	} else if _, err := decodeBlob(s, maxSegmentRawBytes); err != nil {
		t.Fatalf("a segment at the limit must decode: %v", err)
	}
}

// noFCSFrame returns a valid frame without a content size: a streaming
// writer with many flushes.
func noFCSFrame(t *testing.T, raw []byte, parts int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
	for i := 0; i < parts; i++ {
		w.Write(raw)
		w.Flush()
	}
	w.Close()
	var h zstd.Header
	if err := h.Decode(buf.Bytes()); err != nil || h.HasFCS {
		t.Fatalf("test frame should lack FCS: %v %+v", err, h)
	}
	return buf.Bytes()
}

func cat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// TestDecodeRejectsCorruption: anything but exactly one valid frame is
// corruption, in both the in-memory and the streaming decoder.
func TestDecodeRejectsCorruption(t *testing.T) {
	raw := pageLike(64<<10, 4)
	enc := encodeBlob(raw)
	small := encodeBlob([]byte("tiny")) // below 256 bytes: no content size
	empty := encodeBlob(nil)
	big := encodeBlob(bytes.Repeat([]byte("x"), 1024))
	skip := []byte{0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0} // empty skippable frame
	nofcs := noFCSFrame(t, raw, 2)
	flip := append([]byte{}, enc...)
	flip[len(flip)/2] ^= 0xFF
	crc := append([]byte{}, enc...)
	crc[len(crc)-1] ^= 0xFF // checksum is the last 4 bytes
	cases := map[string][]byte{
		"truncated":         enc[:len(enc)/2],
		"header only":       enc[:6],
		"magic only":        enc[:4],
		"bit flip":          flip,
		"bad checksum":      crc,
		"trailing data":     cat(enc, []byte("garbage")),
		"second frame":      cat(enc, enc),
		"small+small":       cat(small, small),
		"big+empty":         cat(big, empty),
		"big+skippable":     cat(big, skip),
		"skippable first":   cat(skip, big),
		"nofcs+frame":       cat(nofcs, small),
		"nofcs+garbage":     cat(nofcs, []byte{0}),
		"nofcs truncated":   nofcs[:len(nofcs)-1],
		"trailing one byte": cat(enc, []byte{0}),
	}
	for name, b := range cases {
		if got, err := decodeBlob(b, maxSegmentRawBytes); !errors.Is(err, ErrCorruptObject) {
			t.Errorf("%s: want ErrCorruptObject, got %d bytes, %v", name, len(got), err)
		}
		if n, err := decompressStream(io.Discard, bytes.NewReader(b), maxBaseRawBytes); !errors.Is(err, ErrCorruptObject) {
			t.Errorf("%s stream: want ErrCorruptObject, got %d bytes, %v", name, n, err)
		}
	}
	// the single valid frames of the cases above all decode
	for name, b := range map[string][]byte{"enc": enc, "small": small, "empty": empty, "big": big, "nofcs": nofcs} {
		if _, err := decodeBlob(b, maxSegmentRawBytes); err != nil {
			t.Errorf("%s alone must decode: %v", name, err)
		}
	}
	// a frame without a declared content size still obeys the limit
	if got, err := decodeBlob(nofcs, maxSegmentRawBytes); err != nil || !bytes.Equal(got, cat(raw, raw)) {
		t.Errorf("no FCS within limit: %d %v", len(got), err)
	}
	if _, err := decodeBlob(nofcs, int64(len(raw))); !errors.Is(err, ErrCorruptObject) {
		t.Errorf("no FCS over limit: want ErrCorruptObject, got %v", err)
	}
}

// TestDecodeBombLimit: a frame that declares more than the limit is
// refused from its header, before it decodes anything.
func TestDecodeBombLimit(t *testing.T) {
	bomb := encodeBlob(make([]byte, 16<<20)) // zeros: tiny frame
	if len(bomb) > 64<<10 {
		t.Fatalf("bomb frame unexpectedly large: %d", len(bomb))
	}
	var w countingWriter
	w.w = io.Discard
	if _, err := decompressStream(&w, bytes.NewReader(bomb), 1<<20); !errors.Is(err, ErrCorruptObject) || w.n != 0 {
		t.Fatalf("bomb: want ErrCorruptObject before output, got %v after %d bytes", err, w.n)
	}
	// a frame header that asks for a window above the cap is refused
	var buf bytes.Buffer
	enc, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(32<<20))
	enc.Write(pageLike(40<<20, 9))
	enc.Close()
	if frameWindow(t, buf.Bytes()) <= maxDecodeWindow {
		t.Fatal("test frame should ask for a large window")
	}
	if _, err := decodeBlob(buf.Bytes(), maxSegmentRawBytes); !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("large window: want ErrCorruptObject, got %v", err)
	}
}

func TestCodecStreamRoundTrip(t *testing.T) {
	// larger than the window, so the frame spans many blocks
	raw := pageLike(5<<20+123, 5)
	var stored bytes.Buffer
	n, err := compressStream(&stored, bytes.NewReader(raw), int64(len(raw)), maxBaseRawBytes)
	if err != nil || n != int64(stored.Len()) || string(stored.Bytes()[:4]) != zstdMagic {
		t.Fatalf("compress: n=%d err=%v", n, err)
	}
	if w := frameWindow(t, stored.Bytes()); w > codecWindow {
		t.Fatalf("window %d above %d", w, codecWindow)
	}
	if stored.Len() >= len(raw)/2 {
		t.Fatalf("poor ratio: %d of %d", stored.Len(), len(raw))
	}
	var out bytes.Buffer
	got, err := decompressStream(&out, bytes.NewReader(stored.Bytes()), maxBaseRawBytes)
	if err != nil || got != int64(len(raw)) || !bytes.Equal(out.Bytes(), raw) {
		t.Fatalf("decompress: n=%d err=%v", got, err)
	}
	// empty payload: still one frame
	stored.Reset()
	if _, err := compressStream(&stored, bytes.NewReader(nil), 0, maxBaseRawBytes); err != nil {
		t.Fatal(err)
	}
	if n, err := decompressStream(io.Discard, bytes.NewReader(stored.Bytes()), maxBaseRawBytes); err != nil || n != 0 {
		t.Fatalf("empty: n=%d err=%v", n, err)
	}
	// a short read of the source is reported, not silently compressed
	if _, err := compressStream(&bytes.Buffer{}, bytes.NewReader(raw[:100]), int64(len(raw)), maxBaseRawBytes); err == nil {
		t.Fatal("short source must fail")
	}
	// the writer refuses what the reader would refuse
	if _, err := compressStream(&bytes.Buffer{}, bytes.NewReader(raw), int64(len(raw)), 1<<20); err == nil {
		t.Fatal("writer must refuse a payload above the reader limit")
	}
}

type patternReader struct{ n, off int64 }

func (p *patternReader) Read(b []byte) (int, error) {
	if p.off >= p.n {
		return 0, io.EOF
	}
	if int64(len(b)) > p.n-p.off {
		b = b[:p.n-p.off]
	}
	for i := range b {
		b[i] = byte((p.off + int64(i)) % 251)
	}
	p.off += int64(len(b))
	return len(b), nil
}

// TestCodecLargeFrames: frames far above every window and buffer still
// decode by streaming: a base (stream encoder) and a segment (block
// encoder) above the decoder window cap.
func TestCodecLargeFrames(t *testing.T) {
	if testing.Short() {
		t.Skip("large frames")
	}
	const n = 128<<20 + 12345
	var sb bytes.Buffer
	if _, err := compressStream(&sb, &patternReader{n: n}, n, maxBaseRawBytes); err != nil {
		t.Fatal(err)
	}
	got, err := decompressStream(io.Discard, bytes.NewReader(sb.Bytes()), maxBaseRawBytes)
	if err != nil || got != n {
		t.Fatalf("stream n=%d err=%v", got, err)
	}
	seg := pageLike(maxDecodeWindow*2+77, 10)
	enc, err := segmentObject(seg, manifestFormatZstd)
	if err != nil {
		t.Fatal(err)
	}
	if w := frameWindow(t, enc); w > codecWindow {
		t.Fatalf("segment window %d above %d", w, codecWindow)
	}
	out, err := decodeBlob(enc, maxSegmentRawBytes)
	if err != nil || !bytes.Equal(out, seg) {
		t.Fatalf("segment: %v", err)
	}
}

// TestFetchDecodedKeepsStoreErrors: a missing object stays ErrNotFound
// (recovery maps it to a stale-generation retry), and a corrupt one is
// ErrCorruptObject.
func TestFetchDecodedKeepsStoreErrors(t *testing.T) {
	fs := NewFakeStore()
	ctx := context.Background()
	if _, _, err := fetchDecodedTo(ctx, fs, "fake/nope", &bytes.Buffer{}, maxBaseRawBytes); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	raw := pageLike(1<<20, 8)
	var sb bytes.Buffer
	compressStream(&sb, bytes.NewReader(raw), int64(len(raw)), maxBaseRawBytes)
	fs.PutBase(ctx, "fake/ok", bytes.NewReader(sb.Bytes()), int64(sb.Len()))
	var out bytes.Buffer
	n, st, err := fetchDecodedTo(ctx, fs, "fake/ok", &out, maxBaseRawBytes)
	if err != nil || n != int64(len(raw)) || st != int64(sb.Len()) || !bytes.Equal(out.Bytes(), raw) {
		t.Fatalf("fetch: n=%d stored=%d err=%v", n, st, err)
	}
	bad := append([]byte{}, sb.Bytes()...)
	bad[len(bad)/3] ^= 0xFF
	fs.PutBase(ctx, "fake/bad", bytes.NewReader(bad), int64(len(bad)))
	if _, _, err := fetchDecodedTo(ctx, fs, "fake/bad", &bytes.Buffer{}, maxBaseRawBytes); !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("want ErrCorruptObject, got %v", err)
	}
}

// TestCodecConcurrent: the shared block codec and the stream pools are
// safe under concurrent use (run with -race).
func TestCodecConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				raw := pageLike(8<<10+g*97+i, int64(g*100+i))
				enc := encodeBlob(raw)
				got, err := decodeBlob(enc, maxSegmentRawBytes)
				if err != nil || !bytes.Equal(got, raw) {
					errs <- fmt.Errorf("blob g=%d i=%d: %v", g, i, err)
					return
				}
				var sb, out bytes.Buffer
				if _, err := compressStream(&sb, bytes.NewReader(raw), int64(len(raw)), maxBaseRawBytes); err != nil {
					errs <- err
					return
				}
				if _, err := decompressStream(&out, &sb, maxBaseRawBytes); err != nil || !bytes.Equal(out.Bytes(), raw) {
					errs <- fmt.Errorf("stream g=%d i=%d: %v", g, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
