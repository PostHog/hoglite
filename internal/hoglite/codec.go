package hoglite

// Object codec for data-plane objects: WAL segments, base images, and any
// later object type (for example page deltas). Every stored object is
// self-describing. A compressed object is exactly one zstd frame and starts
// with the zstd frame magic. Any other object is raw bytes, as written by
// builds before compression existed. Raw SQLite files start with
// "SQLite format 3\x00" and raw WAL bytes start with a WAL header or a
// frame header, so the two forms do not collide (see segmentObject for the
// one theoretical collision and how the writer avoids it).
//
// Detection by content, not by key or manifest field, is deliberate. Keys
// keep their old shape, so an old build lists and deletes new objects
// exactly as before. A segment key suffix such as ".seg.zst" would make an
// old build skip those segments, replay a short WAL and publish a base
// without them: silent data loss. With content detection an old build
// downloads zstd bytes it cannot parse and refuses the tenant loudly.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const zstdMagic = "\x28\xb5\x2f\xfd"

// maxBlobRawBytes bounds the decoded size of an in-memory object
// (segments; page deltas later). Writers store larger payloads raw, so a
// compressed object above this size is never legitimate. The bound stops a
// corrupt or hostile frame from allocating unbounded memory, and it can
// never make a legitimate object unreadable.
const maxBlobRawBytes = 64 << 20

// maxStreamRawBytes is a sanity ceiling for streamed objects (base
// images). Streams go to disk, not memory. compressStream declares the
// exact content size in the frame, and the reader holds the stream to it,
// so the effective bound is the declared size.
const maxStreamRawBytes = 4 << 40

// streamWindow is the encoder window for streamed objects. The decoder
// rejects frames that ask for more than maxStreamWindow, which bounds the
// memory a corrupt frame header can demand.
const (
	streamWindow    = 2 << 20 // 8 MiB gains <0.1% ratio and costs 12 MiB more per encoder
	maxStreamWindow = 64 << 20
)

// ErrCorruptObject marks a stored object that claims to be compressed but
// does not decode, or decodes to an unexpected size.
var ErrCorruptObject = errors.New("corrupt compressed object")

// Level choice, measured on the hoglake-reimpl schema model (single core):
//   - 1-commit segment, 36 KiB raw: SpeedFastest 4.9x in ~80us,
//     SpeedDefault 5.6x in ~130us.
//   - 7.25 MiB base: SpeedFastest 4.48x in 26ms, SpeedDefault 4.54x in
//     35ms.
//
// Segments are on the commit path and a segment PUT costs one round trip
// at any of these sizes, so CPU matters more than the last 10% of ratio.
// The base compresses on the tenant actor while writes wait, and the
// default level saves only 1.4% there. Both use SpeedFastest.
const (
	blobLevel   = zstd.SpeedFastest
	streamLevel = zstd.SpeedFastest
)

// blobCodec holds the shared block encoder and decoder. EncodeAll and
// DecodeAll are safe for concurrent use. Each keeps an internal pool of at
// most GOMAXPROCS states, so thousands of tenants share a few states and
// creation cost is paid once per process.
var blobCodec = sync.OnceValues(func() (*zstd.Encoder, *zstd.Decoder) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(blobLevel), zstd.WithEncoderCRC(true))
	if err != nil {
		panic(err) // static options: cannot fail at run time
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxMemory(maxBlobRawBytes))
	if err != nil {
		panic(err)
	}
	return enc, dec
})

// Streaming encoders and decoders are not safe for concurrent use, so each
// stream borrows one from a pool. Concurrency 1 keeps them synchronous: no
// background goroutines, so a dropped one leaks nothing.
var streamEncPool = sync.Pool{New: func() any {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(streamLevel), zstd.WithEncoderCRC(true),
		zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(streamWindow))
	if err != nil {
		panic(err)
	}
	return enc
}}

var streamDecPool = sync.Pool{New: func() any {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxWindow(maxStreamWindow), zstd.WithDecoderMaxMemory(maxStreamWindow))
	if err != nil {
		panic(err)
	}
	return dec
}}

// isZstd reports whether a stored object starts with the zstd frame magic.
func isZstd(b []byte) bool { return len(b) >= 4 && string(b[:4]) == zstdMagic }

// encodeBlob compresses an in-memory payload into one zstd frame with the
// content size and a checksum. It returns ok=false when the payload is too
// large for the reader bound; the caller then stores it raw.
func encodeBlob(raw []byte) (out []byte, ok bool) {
	if len(raw) > maxBlobRawBytes {
		return nil, false
	}
	enc, _ := blobCodec()
	return enc.EncodeAll(raw, make([]byte, 0, len(raw)/4+64)), true
}

// decodeBlob returns the raw payload of a stored object. Raw objects pass
// through unchanged. A compressed object must decode within maxRaw. When
// the frame declares its content size (every frame above 255 bytes that
// encodeBlob writes does), the bound is checked before decoding and the
// decoded length must match it exactly.
func decodeBlob(stored []byte, maxRaw int64) ([]byte, error) {
	if !isZstd(stored) {
		return stored, nil
	}
	var h zstd.Header
	if err := h.Decode(stored); err != nil {
		return nil, fmt.Errorf("%w: header: %v", ErrCorruptObject, err)
	}
	if maxRaw > maxBlobRawBytes {
		maxRaw = maxBlobRawBytes
	}
	if h.HasFCS && h.FrameContentSize > uint64(maxRaw) {
		return nil, fmt.Errorf("%w: declared size %d exceeds limit %d", ErrCorruptObject, h.FrameContentSize, maxRaw)
	}
	var dst []byte
	if h.HasFCS {
		dst = make([]byte, 0, h.FrameContentSize)
	}
	// the shared decoder caps any output at maxBlobRawBytes, so a frame
	// without a declared size cannot allocate beyond that either
	_, dec := blobCodec()
	out, err := dec.DecodeAll(stored, dst)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptObject, err)
	}
	if int64(len(out)) > maxRaw {
		return nil, fmt.Errorf("%w: decoded %d bytes, limit %d", ErrCorruptObject, len(out), maxRaw)
	}
	if h.HasFCS && uint64(len(out)) != h.FrameContentSize {
		// DecodeAll also decodes trailing frames. We write exactly one, so
		// any size other than the declared one is corruption.
		return nil, fmt.Errorf("%w: decoded %d bytes, frame declares %d", ErrCorruptObject, len(out), h.FrameContentSize)
	}
	return out, nil
}

// countingWriter counts bytes that pass through to w.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// compressStream writes src (exactly rawSize bytes) to dst as one zstd
// frame with content size and checksum, without buffering the payload in
// memory. It returns the stored (compressed) byte count.
func compressStream(dst io.Writer, src io.Reader, rawSize int64) (int64, error) {
	enc := streamEncPool.Get().(*zstd.Encoder)
	cw := &countingWriter{w: dst}
	enc.ResetContentSize(cw, rawSize)
	n, err := io.Copy(enc, src)
	if err == nil && n != rawSize {
		err = fmt.Errorf("compress: read %d bytes, expected %d", n, rawSize)
	}
	if err != nil {
		// a failed stream leaves the encoder mid-frame: drop it rather
		// than return a dirty one to the pool
		return 0, err
	}
	if err := enc.Close(); err != nil {
		return 0, err
	}
	enc.Reset(nil) // release dst
	streamEncPool.Put(enc)
	return cw.n, nil
}

// decompressStream copies the raw payload of a stored object from src to
// dst. Raw objects pass through unchanged. A compressed object must decode
// within maxRaw. compressStream always declares the content size; then the
// bound is checked up front and the decoded length must match exactly. It
// returns the raw byte count.
func decompressStream(dst io.Writer, src io.Reader, maxRaw int64) (int64, error) {
	se := &srcErrReader{r: src}
	br := bufio.NewReaderSize(se, 64<<10)
	magic, err := br.Peek(4)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if !isZstd(magic) {
		return io.Copy(dst, br)
	}
	hdr, err := br.Peek(zstd.HeaderMaxSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	var h zstd.Header
	if err := h.Decode(hdr); err != nil {
		return 0, fmt.Errorf("%w: header: %v", ErrCorruptObject, err)
	}
	if maxRaw > maxStreamRawBytes {
		maxRaw = maxStreamRawBytes
	}
	if h.HasFCS && h.FrameContentSize > uint64(maxRaw) {
		return 0, fmt.Errorf("%w: declared size %d exceeds limit %d", ErrCorruptObject, h.FrameContentSize, maxRaw)
	}
	want := int64(-1)
	limit := maxRaw
	if h.HasFCS {
		want = int64(h.FrameContentSize)
		limit = want
	}
	dec := streamDecPool.Get().(*zstd.Decoder)
	if err := dec.Reset(br); err != nil {
		dec.Close()
		return 0, err
	}
	// read one byte past the declared size: a longer stream (trailing
	// frames) must fail instead of silently growing the file
	n, err := io.Copy(dst, io.LimitReader(dec, limit+1))
	if err != nil {
		// a mid-stream error leaves decoder state undefined: drop it
		dec.Close()
		if se.err != nil {
			return n, se.err
		}
		return n, fmt.Errorf("%w: %v", ErrCorruptObject, err)
	}
	if n > limit || (want >= 0 && n != want) {
		dec.Close()
		return n, fmt.Errorf("%w: decoded %d bytes, frame declares %d, limit %d", ErrCorruptObject, n, want, maxRaw)
	}
	_ = dec.Reset(nil) // release src
	streamDecPool.Put(dec)
	return n, nil
}

// srcErrReader remembers the first non-EOF error of the underlying
// source. It separates transport failures (which keep their identity, for
// example ErrNotFound) from decode failures (which are corruption).
type srcErrReader struct {
	r   io.Reader
	err error
}

func (s *srcErrReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil && err != io.EOF && s.err == nil {
		s.err = err
	}
	return n, err
}

// segmentObject returns the bytes to store for one raw WAL segment.
//
// A raw object must never start with the zstd magic, or a reader would
// take it for a frame. Raw WAL at offset 0 starts with the WAL magic. A
// later segment starts with a frame header whose first field is a page
// number; the magic would mean page 682,962,941 (a 2.6 TiB file at 4 KiB
// pages). The writer still closes that gap: such a segment is always
// compressed, whatever the flag says.
func segmentObject(raw []byte, compress bool) []byte {
	collides := isZstd(raw)
	if !compress && !collides {
		return raw
	}
	enc, ok := encodeBlob(raw)
	if !ok {
		if collides {
			// above the reader bound AND colliding: the reader refuses
			// this frame loudly, which is still safer than a misread
			e, _ := blobCodec()
			return e.EncodeAll(raw, nil)
		}
		return raw
	}
	if len(enc) >= len(raw) && !collides {
		return raw // incompressible: raw is smaller and cheaper to read
	}
	return enc
}

// errFetchAborted stops a download whose decoder already failed.
var errFetchAborted = errors.New("fetch aborted by decoder")

// fetchDecodedTo streams a stored object through decompressStream into w.
// It returns the raw and stored byte counts. Store errors (for example
// ErrNotFound) keep their identity; decode errors wrap ErrCorruptObject.
func fetchDecodedTo(ctx context.Context, store Backend, key string, w io.Writer, maxRaw int64) (raw, stored int64, err error) {
	pr, pw := io.Pipe()
	cnt := &countingWriter{w: pw}
	getErr := make(chan error, 1)
	go func() {
		err := store.GetObjectTo(ctx, key, cnt)
		pw.CloseWithError(err) // nil closes with io.EOF
		getErr <- err
	}()
	raw, derr := decompressStream(w, pr, maxRaw)
	pr.CloseWithError(errFetchAborted) // unblocks the writer if we stopped early
	gerr := <-getErr
	if gerr != nil && !errors.Is(gerr, errFetchAborted) {
		return raw, cnt.n, gerr
	}
	return raw, cnt.n, derr
}
