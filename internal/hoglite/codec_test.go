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

func TestCodecBlobRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 100, 36 << 10, 1 << 20} {
		raw := pageLike(n, int64(n))
		enc, ok := encodeBlob(raw)
		if !ok || !isZstd(enc) {
			t.Fatalf("n=%d: not encoded", n)
		}
		got, err := decodeBlob(enc, maxBlobRawBytes)
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("n=%d: round trip failed: %v", n, err)
		}
	}
	// raw objects (old build) pass through untouched
	walHdr := []byte{0x37, 0x7f, 0x06, 0x82, 0, 0x2d, 0xe2, 0x18}
	got, err := decodeBlob(walHdr, maxBlobRawBytes)
	if err != nil || !bytes.Equal(got, walHdr) {
		t.Fatalf("raw passthrough: %v", err)
	}
}

func TestSegmentObjectForms(t *testing.T) {
	compressible := pageLike(36<<10, 1)
	if s := segmentObject(compressible, true); !isZstd(s) || len(s) >= len(compressible) {
		t.Fatalf("compressible segment not compressed: %d bytes", len(s))
	}
	if s := segmentObject(compressible, false); !bytes.Equal(s, compressible) {
		t.Fatal("compress=false must store raw")
	}
	// incompressible bytes stay raw: smaller and cheaper to read
	noise := randomBytes(4096, 2)
	noise[0] = 0 // not the magic
	if s := segmentObject(noise, true); !bytes.Equal(s, noise) {
		t.Fatal("incompressible segment should stay raw")
	}
	// a raw payload that starts with the zstd magic is always encoded, even
	// with compression off, so no reader can take it for a frame
	collide := append([]byte(zstdMagic), randomBytes(4096, 3)...)
	for _, c := range []bool{false, true} {
		s := segmentObject(collide, c)
		if bytes.Equal(s, collide) {
			t.Fatalf("compress=%v: colliding payload stored raw", c)
		}
		got, err := decodeBlob(s, maxBlobRawBytes)
		if err != nil || !bytes.Equal(got, collide) {
			t.Fatalf("compress=%v: colliding payload round trip: %v", c, err)
		}
	}
}

func TestDecodeBlobRejectsCorruption(t *testing.T) {
	raw := pageLike(64<<10, 4)
	enc, _ := encodeBlob(raw)
	cases := map[string][]byte{
		"truncated":     enc[:len(enc)/2],
		"header only":   enc[:6],
		"trailing data": append(append([]byte{}, enc...), enc...),
	}
	flip := append([]byte{}, enc...)
	flip[len(flip)/2] ^= 0xFF
	cases["bit flip"] = flip
	crc := append([]byte{}, enc...)
	crc[len(crc)-1] ^= 0xFF // checksum is the last 4 bytes
	cases["bad checksum"] = crc
	for name, b := range cases {
		if _, err := decodeBlob(b, maxBlobRawBytes); !errors.Is(err, ErrCorruptObject) {
			t.Errorf("%s: want ErrCorruptObject, got %v", name, err)
		}
	}
	// a frame without a declared content size still obeys the limit
	var buf bytes.Buffer
	w, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
	for i := 0; i < 64; i++ { // many writes: no single-shot FCS
		w.Write(raw)
		w.Flush()
	}
	w.Close()
	var h zstd.Header
	if err := h.Decode(buf.Bytes()); err != nil || h.HasFCS {
		t.Fatalf("test frame should lack FCS: %v %+v", err, h)
	}
	if got, err := decodeBlob(buf.Bytes(), maxBlobRawBytes); err != nil || len(got) != 64*len(raw) {
		t.Errorf("no FCS within limit: %d %v", len(got), err)
	}
	if _, err := decodeBlob(buf.Bytes(), int64(len(raw))); !errors.Is(err, ErrCorruptObject) {
		t.Errorf("no FCS over limit: want ErrCorruptObject, got %v", err)
	}
	if _, err := decompressStream(&bytes.Buffer{}, bytes.NewReader(buf.Bytes()), int64(len(raw))); !errors.Is(err, ErrCorruptObject) {
		t.Errorf("stream no FCS over limit: want ErrCorruptObject, got %v", err)
	}
	var out bytes.Buffer
	if n, err := decompressStream(&out, bytes.NewReader(buf.Bytes()), maxStreamRawBytes); err != nil || n != int64(64*len(raw)) {
		t.Errorf("stream no FCS: n=%d err=%v", n, err)
	}
}

// TestDecodeBlobBombLimit: a frame that declares more than the bound is
// refused from its header, before any large allocation.
func TestDecodeBlobBombLimit(t *testing.T) {
	enc, _ := blobCodec()
	bomb := enc.EncodeAll(make([]byte, maxBlobRawBytes+1), nil) // zeros: tiny frame
	if len(bomb) > 64<<10 {
		t.Fatalf("bomb frame unexpectedly large: %d", len(bomb))
	}
	if _, err := decodeBlob(bomb, maxBlobRawBytes); !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("bomb: want ErrCorruptObject, got %v", err)
	}
	small, _ := encodeBlob(make([]byte, 1<<20))
	if _, err := decodeBlob(small, 1<<10); !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("caller limit: want ErrCorruptObject, got %v", err)
	}
	// the writer stores payloads above the bound raw, so the bound never
	// makes a legitimate object unreadable
	big := make([]byte, maxBlobRawBytes+1)
	big[0] = 0x37
	if s := segmentObject(big, true); len(s) != len(big) || isZstd(s) {
		t.Fatal("oversized segment must be stored raw")
	}
}

func TestCodecStreamRoundTrip(t *testing.T) {
	// larger than the stream window, so the frame spans many blocks
	raw := pageLike(5<<20+123, 5)
	var stored bytes.Buffer
	n, err := compressStream(&stored, bytes.NewReader(raw), int64(len(raw)))
	if err != nil || n != int64(stored.Len()) || !isZstd(stored.Bytes()) {
		t.Fatalf("compress: n=%d err=%v", n, err)
	}
	if stored.Len() >= len(raw)/2 {
		t.Fatalf("poor ratio: %d of %d", stored.Len(), len(raw))
	}
	var out bytes.Buffer
	got, err := decompressStream(&out, bytes.NewReader(stored.Bytes()), maxStreamRawBytes)
	if err != nil || got != int64(len(raw)) || !bytes.Equal(out.Bytes(), raw) {
		t.Fatalf("decompress: n=%d err=%v", got, err)
	}
	// raw base images (old build) pass through untouched
	sqlite := append([]byte("SQLite format 3\x00"), pageLike(10000, 6)...)
	out.Reset()
	if _, err := decompressStream(&out, bytes.NewReader(sqlite), maxStreamRawBytes); err != nil || !bytes.Equal(out.Bytes(), sqlite) {
		t.Fatalf("raw passthrough: %v", err)
	}
	// empty object passes through as empty
	out.Reset()
	if n, err := decompressStream(&out, bytes.NewReader(nil), maxStreamRawBytes); err != nil || n != 0 {
		t.Fatalf("empty: n=%d err=%v", n, err)
	}
	// a short read of the source is reported, not silently compressed
	if _, err := compressStream(&bytes.Buffer{}, bytes.NewReader(raw[:100]), int64(len(raw))); err == nil {
		t.Fatal("short source must fail")
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

// TestCodecStreamAboveMemoryCaps: a streamed object larger than every
// in-memory bound (decoder memory cap, blob cap) still round-trips. A
// base image of any size must stay readable.
func TestCodecStreamAboveMemoryCaps(t *testing.T) {
	if testing.Short() {
		t.Skip("large stream")
	}
	const n = maxStreamWindow + maxBlobRawBytes + 12345
	var sb bytes.Buffer
	if _, err := compressStream(&sb, &patternReader{n: n}, n); err != nil {
		t.Fatal(err)
	}
	got, err := decompressStream(io.Discard, bytes.NewReader(sb.Bytes()), maxStreamRawBytes)
	if err != nil || got != n {
		t.Fatalf("n=%d err=%v", got, err)
	}
}

func TestCodecStreamRejectsCorruption(t *testing.T) {
	raw := pageLike(3<<20, 7)
	var sb bytes.Buffer
	if _, err := compressStream(&sb, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatal(err)
	}
	enc := sb.Bytes()
	flip := append([]byte{}, enc...)
	flip[len(flip)/2] ^= 0xFF
	cases := map[string][]byte{
		"truncated":     enc[:len(enc)-100],
		"bit flip":      flip,
		"trailing data": append(append([]byte{}, enc...), []byte("garbage-after-frame")...),
		"second frame":  append(append([]byte{}, enc...), enc...),
	}
	for name, b := range cases {
		if _, err := decompressStream(&bytes.Buffer{}, bytes.NewReader(b), maxStreamRawBytes); !errors.Is(err, ErrCorruptObject) {
			t.Errorf("%s: want ErrCorruptObject, got %v", name, err)
		}
	}
	if _, err := decompressStream(&bytes.Buffer{}, bytes.NewReader(enc), 1<<20); !errors.Is(err, ErrCorruptObject) {
		t.Errorf("limit: want ErrCorruptObject, got %v", err)
	}
}

// TestFetchDecodedKeepsStoreErrors: a missing object stays ErrNotFound
// (recovery maps it to a stale-generation retry), and a corrupt one is
// ErrCorruptObject.
func TestFetchDecodedKeepsStoreErrors(t *testing.T) {
	fs := NewFakeStore()
	ctx := context.Background()
	if _, _, err := fetchDecodedTo(ctx, fs, "fake/nope", &bytes.Buffer{}, maxStreamRawBytes); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	raw := pageLike(1<<20, 8)
	var sb bytes.Buffer
	compressStream(&sb, bytes.NewReader(raw), int64(len(raw)))
	fs.PutBase(ctx, "fake/ok", bytes.NewReader(sb.Bytes()), int64(sb.Len()))
	var out bytes.Buffer
	n, st, err := fetchDecodedTo(ctx, fs, "fake/ok", &out, maxStreamRawBytes)
	if err != nil || n != int64(len(raw)) || st != int64(sb.Len()) || !bytes.Equal(out.Bytes(), raw) {
		t.Fatalf("fetch: n=%d stored=%d err=%v", n, st, err)
	}
	bad := append([]byte{}, sb.Bytes()...)
	bad[len(bad)/3] ^= 0xFF
	fs.PutBase(ctx, "fake/bad", bytes.NewReader(bad), int64(len(bad)))
	if _, _, err := fetchDecodedTo(ctx, fs, "fake/bad", &bytes.Buffer{}, maxStreamRawBytes); !errors.Is(err, ErrCorruptObject) {
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
				enc, _ := encodeBlob(raw)
				got, err := decodeBlob(enc, maxBlobRawBytes)
				if err != nil || !bytes.Equal(got, raw) {
					errs <- fmt.Errorf("blob g=%d i=%d: %v", g, i, err)
					return
				}
				var sb, out bytes.Buffer
				if _, err := compressStream(&sb, bytes.NewReader(raw), int64(len(raw))); err != nil {
					errs <- err
					return
				}
				if _, err := decompressStream(&out, &sb, maxStreamRawBytes); err != nil || !bytes.Equal(out.Bytes(), raw) {
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
