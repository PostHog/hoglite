package hoglite

// Object codec for data-plane objects: WAL segments, base images, and any
// later object type (for example page deltas).
//
// The codec is a property of the GENERATION, not of the object. The
// manifest field Format names it, and it covers the base image and every
// segment of that generation:
//
//   - manifestFormatRaw (0, absent): every object is raw bytes, as builds
//     before compression wrote them. Readers never sniff the content.
//   - manifestFormatZstd (1): every object is exactly one zstd frame.
//     There is no raw fallback for size, incompressibility or any other
//     reason. A raw object in such a generation is corruption.
//   - manifestFormatChain (2): as format 1 (every object one zstd frame),
//     and the start state is the base plus a chain of page deltas
//     (see delta.go).
//
// The writer fixes the format when it publishes a generation (checkpoint
// CAS) and keeps it until the next one, whatever the Compress flag does
// in between.
//
// Why no mixing: a build from before compression ignores Format and reads
// every object raw. In a zstd generation it then always sees a base that
// is not a SQLite file and a first segment that is not a WAL header, so it
// refuses before its claim CAS. One raw segment over a compressed base
// would let SQLite read page 1 from the raw WAL, so the old build would
// open the tenant, fold a broken database and publish it (data loss).
//
// Keys do not change: segment offsets stay RAW WAL offsets. A key suffix
// such as ".seg.zst" would make an old build skip those segments, replay
// a short WAL and publish a base without them.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const zstdMagic = "\x28\xb5\x2f\xfd"

// Size limits. Writer and reader use the SAME constant for each object
// type, so a writer never publishes an object that a reader refuses:
//   - a segment above maxSegmentRawBytes cannot go into a zstd generation.
//     The tenant fails before any ack of the batch (cutSegment).
//   - a base above maxBaseRawBytes is published raw, in a raw generation
//     (prepareBaseUpload).
//
// Readers stream every frame to disk through the decoder, so the limits
// do not set memory use. They stop a corrupt or hostile frame from
// filling the disk.
var maxSegmentRawBytes int64 = 1 << 30 // a var only so tests can lower it

const maxBaseRawBytes = 4 << 40

// codecWindow is the encoder window for every frame we write. The decoder
// refuses frames that ask for more than maxDecodeWindow. A decoder holds
// about two windows, so this bounds decoder memory per active stream
// (3.2 MiB measured for our frames, about 17 MiB for a hostile header).
const (
	codecWindow     = 2 << 20 // 8 MiB gains <0.1% ratio and costs 12 MiB more per encoder
	maxDecodeWindow = 8 << 20
)

// ErrCorruptObject marks a stored object of a zstd generation that does
// not decode, is not exactly one frame, or decodes to an unexpected size.
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
const codecLevel = zstd.SpeedFastest

// blobEnc is the shared encoder for in-memory payloads. EncodeAll is safe
// for concurrent use and keeps an internal pool of at most GOMAXPROCS
// states, so thousands of tenants share a few states and creation cost is
// paid once per process.
var blobEnc = sync.OnceValue(func() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(codecLevel), zstd.WithEncoderCRC(true),
		zstd.WithWindowSize(codecWindow))
	if err != nil {
		panic(err) // static options: cannot fail at run time
	}
	return enc
})

// Streaming encoders and decoders are not safe for concurrent use, so each
// stream borrows one from a pool. Concurrency 1 keeps them synchronous: no
// background goroutines, so a dropped one leaks nothing. The pool does not
// bound how many are active; the materialization semaphore in Manager
// does that for readers, and the checkpoint semaphores do it for writers.
var streamEncPool = sync.Pool{New: func() any {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(codecLevel), zstd.WithEncoderCRC(true),
		zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(codecWindow))
	if err != nil {
		panic(err)
	}
	return enc
}}

var streamDecPool = sync.Pool{New: func() any {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxWindow(maxDecodeWindow), zstd.WithDecoderMaxMemory(maxDecodeWindow))
	if err != nil {
		panic(err)
	}
	return dec
}}

// encodeBlob compresses an in-memory payload into one zstd frame with a
// checksum (and the content size, for payloads of 256 bytes or more). The
// caller enforces the size limit of its object type before it calls.
func encodeBlob(raw []byte) []byte {
	return blobEnc().EncodeAll(raw, make([]byte, 0, len(raw)/4+64))
}

// decodeBlob decodes one zstd frame held in memory, with the same checks
// as decompressStream. maxRaw is the limit of the object type.
func decodeBlob(stored []byte, maxRaw int64) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := decompressStream(&buf, bytes.NewReader(stored), maxRaw); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
// memory. It refuses a payload above maxRaw, the reader limit of the
// object type. It returns the stored (compressed) byte count.
func compressStream(dst io.Writer, src io.Reader, rawSize, maxRaw int64) (int64, error) {
	if rawSize > maxRaw {
		return 0, fmt.Errorf("compress: %d bytes exceeds the reader limit %d", rawSize, maxRaw)
	}
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

// decompressStream decodes exactly one zstd frame from src into dst and
// returns the raw byte count. The object must start with the frame magic
// (no raw passthrough: the generation format says the object is zstd),
// hold exactly one frame and nothing after it (no second frame, empty
// frame or skippable frame), and decode to at most maxRaw bytes. A frame
// that declares its content size is checked against maxRaw before
// decoding and must decode to exactly that size. Source errors keep their
// identity; every other failure wraps ErrCorruptObject.
func decompressStream(dst io.Writer, src io.Reader, maxRaw int64) (int64, error) {
	se := &srcErrReader{r: src}
	fr := &frameReader{br: bufio.NewReaderSize(se, 64<<10)}
	n, err := decodeFrame(dst, fr, maxRaw)
	if err != nil && se.err != nil {
		return n, se.err // transport failure, not corruption
	}
	return n, err
}

func decodeFrame(dst io.Writer, fr *frameReader, maxRaw int64) (int64, error) {
	hdr, err := fr.br.Peek(zstd.HeaderMaxSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if len(hdr) < 4 || string(hdr[:4]) != zstdMagic {
		return 0, fmt.Errorf("%w: no zstd frame magic", ErrCorruptObject)
	}
	var h zstd.Header
	if err := h.Decode(hdr); err != nil {
		return 0, fmt.Errorf("%w: header: %v", ErrCorruptObject, err)
	}
	if h.HasFCS && h.FrameContentSize > uint64(maxRaw) {
		return 0, fmt.Errorf("%w: declared size %d exceeds limit %d", ErrCorruptObject, h.FrameContentSize, maxRaw)
	}
	want, limit := int64(-1), maxRaw
	if h.HasFCS {
		want = int64(h.FrameContentSize)
		limit = want
	}
	dec := streamDecPool.Get().(*zstd.Decoder)
	if err := dec.Reset(fr); err != nil {
		dec.Close()
		return 0, corruptf(fr, err)
	}
	// read one byte past the limit: a longer output fails instead of
	// silently growing the file
	n, err := io.Copy(dst, io.LimitReader(dec, limit+1))
	if err != nil {
		// a mid-stream error leaves decoder state undefined: drop it
		dec.Close()
		return n, corruptf(fr, err)
	}
	_ = dec.Reset(nil) // release fr
	streamDecPool.Put(dec)
	if n > limit || (want >= 0 && n != want) {
		return n, fmt.Errorf("%w: decoded %d bytes, frame declares %d, limit %d", ErrCorruptObject, n, want, maxRaw)
	}
	// the decoder stopped where frameReader said the frame ends; now the
	// object must end there too
	if !fr.done() {
		return n, fmt.Errorf("%w: decoder stopped inside the frame", ErrCorruptObject)
	}
	if _, err := fr.br.Peek(1); err == nil {
		return n, fmt.Errorf("%w: data after the frame", ErrCorruptObject)
	} else if !errors.Is(err, io.EOF) {
		return n, err
	}
	return n, nil
}

// corruptf wraps a decode failure. A framing error found by frameReader
// is more precise than what the decoder makes of it, so it wins.
func corruptf(fr *frameReader, err error) error {
	if fr.err != nil && !errors.Is(fr.err, io.EOF) {
		return fr.err
	}
	return fmt.Errorf("%w: %v", ErrCorruptObject, err)
}

// frameReader passes exactly one zstd frame from br to the decoder and
// then reports io.EOF. It walks the frame layout of RFC 8878: frame
// header, blocks until the last-block flag, then the optional 4-byte
// checksum. So the decoder never sees a second frame, and the caller can
// check that the object ends where the frame ends.
type frameReader struct {
	br    *bufio.Reader
	left  int64 // bytes of the current section not yet passed on
	state int
	sum   bool  // frame has a content checksum
	err   error // sticky
}

const (
	frHeader = iota
	frBlock
	frChecksum
	frEnd
	frDone
)

const maxBlockBytes = 128 << 10 // Block_Maximum_Size, RFC 8878 3.1.1.2.3

func (f *frameReader) done() bool { return f.state == frDone && f.left == 0 }

func (f *frameReader) Read(p []byte) (int, error) {
	for f.left == 0 {
		if f.err != nil {
			return 0, f.err
		}
		f.err = f.next()
	}
	if int64(len(p)) > f.left {
		p = p[:f.left]
	}
	n, err := f.br.Read(p)
	f.left -= int64(n)
	if err != nil && f.left > 0 {
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("%w: truncated frame", ErrCorruptObject)
		}
		f.err = err
		return n, err
	}
	return n, nil
}

// next computes the length of the next section and moves the state on.
func (f *frameReader) next() error {
	switch f.state {
	case frHeader:
		b, err := f.peek(5)
		if err != nil {
			return err
		}
		if string(b[:4]) != zstdMagic {
			return fmt.Errorf("%w: no zstd frame magic", ErrCorruptObject)
		}
		fhd := b[4]
		if fhd&(1<<3) != 0 {
			return fmt.Errorf("%w: reserved header bit set", ErrCorruptObject)
		}
		if fhd&3 != 0 {
			return fmt.Errorf("%w: dictionary frames are not used", ErrCorruptObject)
		}
		single := fhd&(1<<5) != 0
		f.sum = fhd&(1<<2) != 0
		n := int64(5)
		if !single {
			n++ // window descriptor
		}
		switch fhd >> 6 {
		case 0:
			if single {
				n++
			}
		case 1:
			n += 2
		case 2:
			n += 4
		case 3:
			n += 8
		}
		f.left, f.state = n, frBlock
	case frBlock:
		b, err := f.peek(3)
		if err != nil {
			return err
		}
		v := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
		last, typ, size := v&1 == 1, (v>>1)&3, int64(v>>3)
		if size > maxBlockBytes {
			return fmt.Errorf("%w: block size %d", ErrCorruptObject, size)
		}
		switch typ {
		case 0, 2: // raw, compressed
		case 1: // RLE: one byte repeated size times
			size = 1
		default:
			return fmt.Errorf("%w: reserved block type", ErrCorruptObject)
		}
		f.left = 3 + size
		if last {
			f.state = frChecksum
		}
	case frChecksum:
		if f.sum {
			f.left = 4
		}
		f.state = frEnd
	case frEnd:
		f.state = frDone
		return io.EOF
	case frDone:
		return io.EOF
	}
	return nil
}

func (f *frameReader) peek(n int) ([]byte, error) {
	b, err := f.br.Peek(n)
	if err == nil {
		return b, nil
	}
	if errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: truncated frame", ErrCorruptObject)
	}
	return nil, err
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

// errSegmentTooLarge means a batch is too large for one segment of a zstd
// generation: no reader would accept it.
var errSegmentTooLarge = errors.New("segment exceeds the zstd segment limit")

// segmentObject returns the bytes to store for one raw WAL segment in a
// generation of the given format. A zstd generation gets one frame for
// every segment, including incompressible ones: never raw (see the file
// comment). It refuses a segment above maxSegmentRawBytes rather than
// publish an object that recovery rejects.
func segmentObject(raw []byte, format int) ([]byte, error) {
	if format == manifestFormatRaw {
		return raw, nil
	}
	if int64(len(raw)) > maxSegmentRawBytes {
		return nil, fmt.Errorf("%w: %d bytes, limit %d", errSegmentTooLarge, len(raw), maxSegmentRawBytes)
	}
	return encodeBlob(raw), nil
}

// errFetchAborted stops a download whose decoder already failed.
var errFetchAborted = errors.New("fetch aborted by decoder")

// fetchObjectTo copies the raw payload of a stored object of a generation
// with the given format into w. It returns the raw and stored byte counts.
// A raw-format object is copied as it is, exactly as builds before
// compression did. A zstd-format object must be one frame within maxRaw.
// Store errors (for example ErrNotFound) keep their identity; decode
// errors wrap ErrCorruptObject.
func fetchObjectTo(ctx context.Context, store Backend, key string, w io.Writer, format int, maxRaw int64) (raw, stored int64, err error) {
	if format == manifestFormatRaw {
		cw := &countingWriter{w: w}
		err := store.GetObjectTo(ctx, key, cw)
		return cw.n, cw.n, err
	}
	return fetchDecodedTo(ctx, store, key, w, maxRaw)
}

// fetchDecodedTo streams a stored zstd object through decompressStream
// into w. The compressed object never sits whole in memory.
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
