package hoglite

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// WAL parsing. We ship raw byte ranges of the SQLite WAL file. SQLite may
// overwrite tail bytes in place after a rollback or write stale frames
// beyond the committed end, so the file size is NOT the committed length.
// The WAL checksum chain is the authority: we parse frames from the header
// and stop at the first frame whose salt or cumulative checksum fails.
// The committed end is the end of the last valid commit frame.
//
// Reference: SQLite file format, WAL section. Header is 32 bytes. Frame is
// a 24-byte header plus one page. All header fields are big-endian. The
// checksum endianness is selected by the magic number.

const (
	walMagicLE = 0x377f0682 // checksums little-endian
	walMagicBE = 0x377f0683 // checksums big-endian
	walHdrSize = 32
	walFrmHdr  = 24
)

// walCommitEnd parses wal (the whole WAL file prefix) and returns the byte
// offset just past the last valid commit frame. Returns 0 when no valid
// commit exists. It never errors: any corruption simply ends the chain.
func walCommitEnd(wal []byte) int64 {
	if len(wal) < walHdrSize {
		return 0
	}
	magic := binary.BigEndian.Uint32(wal[0:4])
	var bo binary.ByteOrder
	switch magic {
	case walMagicLE:
		bo = binary.LittleEndian
	case walMagicBE:
		bo = binary.BigEndian
	default:
		return 0
	}
	pageSize := int64(binary.BigEndian.Uint32(wal[8:12]))
	if pageSize == 1 {
		pageSize = 65536 // 0x10000 stored as 1? no: stored directly; guard anyway
	}
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 {
		return 0
	}
	salt1 := binary.BigEndian.Uint32(wal[16:20])
	salt2 := binary.BigEndian.Uint32(wal[20:24])

	// header checksum over first 24 bytes
	s1, s2 := walChecksum(bo, wal[0:24], 0, 0)
	if s1 != binary.BigEndian.Uint32(wal[24:28]) || s2 != binary.BigEndian.Uint32(wal[28:32]) {
		return 0
	}

	frameSize := walFrmHdr + pageSize
	var commitEnd int64
	for off := int64(walHdrSize); off+frameSize <= int64(len(wal)); off += frameSize {
		h := wal[off : off+walFrmHdr]
		if binary.BigEndian.Uint32(h[8:12]) != salt1 || binary.BigEndian.Uint32(h[12:16]) != salt2 {
			break // salt mismatch: chain ends (rollback tail or tear)
		}
		s1, s2 = walChecksum(bo, h[0:8], s1, s2)
		s1, s2 = walChecksum(bo, wal[off+walFrmHdr:off+frameSize], s1, s2)
		if s1 != binary.BigEndian.Uint32(h[16:20]) || s2 != binary.BigEndian.Uint32(h[20:24]) {
			break // checksum mismatch: chain ends
		}
		if binary.BigEndian.Uint32(h[4:8]) != 0 { // dbsize != 0: commit frame
			commitEnd = off + frameSize
		}
	}
	return commitEnd
}

// walCommitEndReader is walCommitEnd over a stream. It holds one frame in
// memory, so recovery can validate a WAL of any size (segments go up to
// 1 GiB) without reading it whole. Read errors other than EOF are
// returned; a short tail simply ends the chain, as in walCommitEnd.
func walCommitEndReader(r io.Reader) (int64, error) {
	br := bufio.NewReaderSize(r, 256<<10)
	hdr := make([]byte, walHdrSize)
	if _, err := io.ReadFull(br, hdr); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil
		}
		return 0, err
	}
	var bo binary.ByteOrder
	switch binary.BigEndian.Uint32(hdr[0:4]) {
	case walMagicLE:
		bo = binary.LittleEndian
	case walMagicBE:
		bo = binary.BigEndian
	default:
		return 0, nil
	}
	pageSize := int64(binary.BigEndian.Uint32(hdr[8:12]))
	if pageSize == 1 {
		pageSize = 65536
	}
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 {
		return 0, nil
	}
	salt1 := binary.BigEndian.Uint32(hdr[16:20])
	salt2 := binary.BigEndian.Uint32(hdr[20:24])
	s1, s2 := walChecksum(bo, hdr[0:24], 0, 0)
	if s1 != binary.BigEndian.Uint32(hdr[24:28]) || s2 != binary.BigEndian.Uint32(hdr[28:32]) {
		return 0, nil
	}
	frameSize := walFrmHdr + pageSize
	frame := make([]byte, frameSize)
	var commitEnd int64
	for off := int64(walHdrSize); ; off += frameSize {
		if _, err := io.ReadFull(br, frame); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return 0, err
		}
		h := frame[:walFrmHdr]
		if binary.BigEndian.Uint32(h[8:12]) != salt1 || binary.BigEndian.Uint32(h[12:16]) != salt2 {
			break
		}
		s1, s2 = walChecksum(bo, h[0:8], s1, s2)
		s1, s2 = walChecksum(bo, frame[walFrmHdr:], s1, s2)
		if s1 != binary.BigEndian.Uint32(h[16:20]) || s2 != binary.BigEndian.Uint32(h[20:24]) {
			break
		}
		if binary.BigEndian.Uint32(h[4:8]) != 0 {
			commitEnd = off + frameSize
		}
	}
	return commitEnd, nil
}

// walChecksum is SQLite's WAL checksum: s1 += x0 + s2; s2 += x1 + s1 over
// 8-byte words, cumulative across the chain.
func walChecksum(bo binary.ByteOrder, data []byte, s1, s2 uint32) (uint32, uint32) {
	for i := 0; i+8 <= len(data); i += 8 {
		s1 += bo.Uint32(data[i:]) + s2
		s2 += bo.Uint32(data[i+4:]) + s1
	}
	return s1, s2
}

// walParser is the incremental form of walCommitEnd. Frames after the last
// commit frame are never cached: a rollback of the open transaction may
// rewrite them in place, but bytes at or before the last commit frame are
// immutable within a WAL incarnation. So revalidation starts exactly at the
// last commit frame and the amortized cost per flush is the new frames only.
type walParser struct {
	bo        binary.ByteOrder
	pageSize  int64
	salt1     uint32
	salt2     uint32
	commitOff int64  // offset AFTER the last verified commit frame
	s1, s2    uint32 // chain state at commitOff
	valid     bool
}

// Reset discards all cached state (after a TRUNCATE checkpoint).
func (p *walParser) Reset() { *p = walParser{} }

// CommitEnd returns the committed WAL end in absolute file offset.
// When p.valid, wal may be a TAIL slice starting at p.commitOff (callers
// avoid re-reading the whole file). When !p.valid, wal must be the whole
// file (or at least the header plus new frames).
func (p *walParser) CommitEnd(wal []byte) int64 {
	if !p.valid {
		if len(wal) < walHdrSize {
			return 0
		}
		magic := binary.BigEndian.Uint32(wal[0:4])
		switch magic {
		case walMagicLE:
			p.bo = binary.LittleEndian
		case walMagicBE:
			p.bo = binary.BigEndian
		default:
			return 0
		}
		p.pageSize = int64(binary.BigEndian.Uint32(wal[8:12]))
		if p.pageSize == 1 {
			p.pageSize = 65536
		}
		if p.pageSize < 512 || p.pageSize > 65536 || p.pageSize&(p.pageSize-1) != 0 {
			return 0
		}
		p.salt1 = binary.BigEndian.Uint32(wal[16:20])
		p.salt2 = binary.BigEndian.Uint32(wal[20:24])
		s1, s2 := walChecksum(p.bo, wal[0:24], 0, 0)
		if s1 != binary.BigEndian.Uint32(wal[24:28]) || s2 != binary.BigEndian.Uint32(wal[28:32]) {
			return 0
		}
		p.s1, p.s2 = s1, s2
		p.commitOff = walHdrSize // chain position; commitEnd stays 0 until a commit frame
		p.valid = true
		return p.scan(wal[walHdrSize:], walHdrSize)
	}
	return p.scan(wal, p.commitOff)
}

// scan parses frames from baseOff (the slice starts there). Only commit
// frame positions are cached; frames past the last commit may be rewritten
// by a rollback and are re-validated next call.
func (p *walParser) scan(wal []byte, baseOff int64) int64 {
	frameSize := int64(walFrmHdr) + p.pageSize
	s1, s2 := p.s1, p.s2
	off := baseOff
	commitEnd := p.commitOff
	for off+frameSize <= baseOff+int64(len(wal)) {
		h := wal[off-baseOff : off-baseOff+walFrmHdr]
		if binary.BigEndian.Uint32(h[8:12]) != p.salt1 || binary.BigEndian.Uint32(h[12:16]) != p.salt2 {
			break
		}
		fs1, fs2 := walChecksum(p.bo, h[0:8], s1, s2)
		fs1, fs2 = walChecksum(p.bo, wal[off-baseOff+walFrmHdr:off-baseOff+frameSize], fs1, fs2)
		if fs1 != binary.BigEndian.Uint32(h[16:20]) || fs2 != binary.BigEndian.Uint32(h[20:24]) {
			break
		}
		s1, s2 = fs1, fs2
		off += frameSize
		if binary.BigEndian.Uint32(h[4:8]) != 0 { // commit frame
			p.s1, p.s2 = s1, s2
			p.commitOff = off
			commitEnd = off
		}
	}
	if commitEnd == walHdrSize {
		return 0 // header only, no commits yet
	}
	return commitEnd
}
