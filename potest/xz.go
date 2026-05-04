package potest

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
)

// newXZReader returns a reader that decompresses a single-stream
// XZ archive whose only filter is LZMA2.
//
// It supports the check types None, CRC32, and CRC64. SHA-256 is
// rejected. Multi-stream concatenation is not supported.
func newXZReader(r io.Reader) (io.Reader, error) {
	z := &xzStream{br: bufio.NewReader(r)}
	if err := z.readStreamHeader(); err != nil {
		return nil, err
	}
	return z, nil
}

type xzStream struct {
	br *bufio.Reader

	checkKind byte // 0=none, 1=crc32, 4=crc64
	flags     [2]byte

	block *xzBlock
	done  bool

	// pending output from the current block.
	out []byte
}

func (z *xzStream) Read(p []byte) (int, error) {
	for len(z.out) == 0 {
		if z.done {
			return 0, io.EOF
		}
		if z.block == nil {
			b, err := z.openBlock()
			if err != nil {
				return 0, err
			}
			if b == nil {
				z.done = true
				if err := z.readIndexAndFooter(); err != nil {
					return 0, err
				}
				return 0, io.EOF
			}
			z.block = b
		}
		buf, err := z.block.next()
		if errors.Is(err, io.EOF) {
			if err := z.block.finish(); err != nil {
				return 0, err
			}
			z.block = nil
			continue
		}
		if err != nil {
			return 0, err
		}
		z.out = buf
	}
	n := copy(p, z.out)
	z.out = z.out[n:]
	return n, nil
}

const xzMagic = "\xfd7zXZ\x00"

func (z *xzStream) readStreamHeader() error {
	var hdr [12]byte
	if _, err := io.ReadFull(z.br, hdr[:]); err != nil {
		return fmt.Errorf("xz: stream header: %w", err)
	}
	if string(hdr[:6]) != xzMagic {
		return errors.New("xz: bad magic")
	}
	if hdr[6] != 0 {
		return errors.New("xz: reserved stream flag set")
	}
	z.checkKind = hdr[7]
	switch z.checkKind {
	case 0x00, 0x01, 0x04:
	default:
		return fmt.Errorf("xz: unsupported check type %#x", z.checkKind)
	}
	z.flags = [2]byte{hdr[6], hdr[7]}
	want := binary.LittleEndian.Uint32(hdr[8:12])
	if got := crc32.ChecksumIEEE(hdr[6:8]); got != want {
		return errors.New("xz: stream header CRC32 mismatch")
	}
	return nil
}

func (z *xzStream) openBlock() (*xzBlock, error) {
	first, err := z.br.ReadByte()
	if err != nil {
		return nil, err
	}
	if first == 0x00 {
		// Index indicator: rewind so readIndexAndFooter can consume it.
		if err := z.br.UnreadByte(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	headerSize := (int(first) + 1) * 4
	hdr := make([]byte, headerSize)
	hdr[0] = first
	if _, err := io.ReadFull(z.br, hdr[1:]); err != nil {
		return nil, fmt.Errorf("xz: block header: %w", err)
	}
	want := binary.LittleEndian.Uint32(hdr[headerSize-4:])
	if got := crc32.ChecksumIEEE(hdr[:headerSize-4]); got != want {
		return nil, errors.New("xz: block header CRC32 mismatch")
	}
	flags := hdr[1]
	numFilters := int(flags&0x03) + 1
	if flags&0x3C != 0 {
		return nil, errors.New("xz: reserved block flag set")
	}
	hasCompressed := flags&0x40 != 0
	hasUncompressed := flags&0x80 != 0

	pos := 2
	rd := &cursor{b: hdr[pos : headerSize-4]}
	if hasCompressed {
		if _, err := rd.uvarint(); err != nil {
			return nil, fmt.Errorf("xz: compressed size: %w", err)
		}
	}
	if hasUncompressed {
		if _, err := rd.uvarint(); err != nil {
			return nil, fmt.Errorf("xz: uncompressed size: %w", err)
		}
	}
	if numFilters != 1 {
		return nil, fmt.Errorf("xz: unsupported filter count %d", numFilters)
	}
	id, err := rd.uvarint()
	if err != nil {
		return nil, fmt.Errorf("xz: filter id: %w", err)
	}
	if id != 0x21 {
		return nil, fmt.Errorf("xz: unsupported filter id %#x", id)
	}
	propSize, err := rd.uvarint()
	if err != nil {
		return nil, fmt.Errorf("xz: filter prop size: %w", err)
	}
	if propSize != 1 {
		return nil, fmt.Errorf("xz: unexpected filter prop size %d", propSize)
	}
	filterProp, err := rd.byte()
	if err != nil {
		return nil, err
	}
	for _, b := range rd.rest() {
		if b != 0 {
			return nil, errors.New("xz: bad header padding")
		}
	}

	dictMax, err := lzma2DictSize(filterProp)
	if err != nil {
		return nil, err
	}
	b := &xzBlock{
		stream:    z,
		hashedLen: 0,
		dictMax:   dictMax,
		raw:       newCounter(z.br),
	}
	b.lz2 = newLZMA2Reader(b.raw, dictMax)
	switch z.checkKind {
	case 0x01:
		b.crc32 = crc32.NewIEEE()
	case 0x04:
		b.crc64 = crc64.New(crc64.MakeTable(crc64.ECMA))
	}
	return b, nil
}

func lzma2DictSize(b byte) (int, error) {
	if b > 40 {
		return 0, fmt.Errorf("xz: invalid lzma2 dict prop %d", b)
	}
	if b == 40 {
		return 1 << 32, nil
	}
	v := uint32(2 | (b & 1))
	v <<= b/2 + 11
	return int(v), nil
}

type xzBlock struct {
	stream  *xzStream
	dictMax int
	raw     *counter
	lz2     *lzma2Reader

	hashedLen int64
	crc32     interface{ io.Writer; Sum32() uint32 }
	crc64     interface{ io.Writer; Sum64() uint64 }
	buf       [4096]byte
}

func (b *xzBlock) next() ([]byte, error) {
	n, err := b.lz2.Read(b.buf[:])
	if n > 0 {
		b.hashedLen += int64(n)
		if b.crc32 != nil {
			b.crc32.Write(b.buf[:n])
		}
		if b.crc64 != nil {
			b.crc64.Write(b.buf[:n])
		}
		return b.buf[:n], nil
	}
	if err == nil {
		err = io.ErrNoProgress
	}
	return nil, err
}

func (b *xzBlock) finish() error {
	used := b.raw.n
	pad := (4 - (used & 3)) & 3
	for range pad {
		bb, err := b.stream.br.ReadByte()
		if err != nil {
			return err
		}
		if bb != 0 {
			return errors.New("xz: bad block padding")
		}
	}
	switch b.stream.checkKind {
	case 0x00:
	case 0x01:
		var c [4]byte
		if _, err := io.ReadFull(b.stream.br, c[:]); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(c[:]) != b.crc32.Sum32() {
			return errors.New("xz: block CRC32 mismatch")
		}
	case 0x04:
		var c [8]byte
		if _, err := io.ReadFull(b.stream.br, c[:]); err != nil {
			return err
		}
		if binary.LittleEndian.Uint64(c[:]) != b.crc64.Sum64() {
			return errors.New("xz: block CRC64 mismatch")
		}
	}
	return nil
}

func (z *xzStream) readIndexAndFooter() error {
	indicator, err := z.br.ReadByte()
	if err != nil {
		return err
	}
	if indicator != 0x00 {
		return errors.New("xz: bad index indicator")
	}
	c := newCounter(z.br)
	rd := &uvarintReader{r: c}
	nrec, err := rd.read()
	if err != nil {
		return err
	}
	for range nrec {
		if _, err := rd.read(); err != nil {
			return err
		}
		if _, err := rd.read(); err != nil {
			return err
		}
	}
	// Index size includes the indicator byte.
	idxBytes := c.n + 1
	pad := (4 - (idxBytes & 3)) & 3
	for range pad {
		bb, err := z.br.ReadByte()
		if err != nil {
			return err
		}
		if bb != 0 {
			return errors.New("xz: bad index padding")
		}
	}
	var crc [4]byte
	if _, err := io.ReadFull(z.br, crc[:]); err != nil {
		return err
	}
	var foot [12]byte
	if _, err := io.ReadFull(z.br, foot[:]); err != nil {
		return err
	}
	if string(foot[10:]) != "YZ" {
		return errors.New("xz: bad footer magic")
	}
	if foot[8] != z.flags[0] || foot[9] != z.flags[1] {
		return errors.New("xz: footer flags mismatch")
	}
	want := binary.LittleEndian.Uint32(foot[:4])
	if got := crc32.ChecksumIEEE(foot[4:10]); got != want {
		return errors.New("xz: stream footer CRC32 mismatch")
	}
	return nil
}

// counter wraps io.ByteReader to track bytes consumed.
type counter struct {
	br interface {
		io.Reader
		io.ByteReader
	}
	n int
}

func newCounter(br interface {
	io.Reader
	io.ByteReader
}) *counter {
	return &counter{br: br}
}

func (c *counter) Read(p []byte) (int, error) {
	n, err := c.br.Read(p)
	c.n += n
	return n, err
}

func (c *counter) ReadByte() (byte, error) {
	b, err := c.br.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}

type cursor struct {
	b []byte
	i int
}

func (c *cursor) byte() (byte, error) {
	if c.i >= len(c.b) {
		return 0, io.ErrUnexpectedEOF
	}
	b := c.b[c.i]
	c.i++
	return b, nil
}

func (c *cursor) uvarint() (uint64, error) {
	var v uint64
	for s := uint(0); s < 63; s += 7 {
		b, err := c.byte()
		if err != nil {
			return 0, err
		}
		v |= uint64(b&0x7F) << s
		if b&0x80 == 0 {
			if b == 0 && s != 0 {
				return 0, errors.New("xz: non-canonical uvarint")
			}
			return v, nil
		}
	}
	return 0, errors.New("xz: uvarint overflow")
}

func (c *cursor) rest() []byte { return c.b[c.i:] }

type uvarintReader struct {
	r io.ByteReader
}

func (u *uvarintReader) read() (uint64, error) {
	var v uint64
	for s := uint(0); s < 63; s += 7 {
		b, err := u.r.ReadByte()
		if err != nil {
			return 0, err
		}
		v |= uint64(b&0x7F) << s
		if b&0x80 == 0 {
			return v, nil
		}
	}
	return 0, errors.New("xz: uvarint overflow")
}

// ------------------------------------------------------------------
// LZMA2 framing
// ------------------------------------------------------------------

type lzma2Reader struct {
	br *counter
	d  *dict

	dec    *lzmaDecoder
	state  int // 0 = expect control byte, 1 = inside chunk, 2 = end
	remain int // bytes left in current uncompressed chunk
	lit    bool
}

func newLZMA2Reader(r *counter, dictMax int) *lzma2Reader {
	d := &dict{buf: make([]byte, 0, 1<<16)}
	return &lzma2Reader{
		br:  r,
		d:   d,
		dec: &lzmaDecoder{d: d},
	}
}

func (r *lzma2Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if r.d.has() {
			n := r.d.read(p)
			return n, nil
		}
		switch r.state {
		case 2:
			return 0, io.EOF
		case 0:
			if err := r.openChunk(); err != nil {
				return 0, err
			}
		case 1:
			if err := r.fillChunk(); err != nil {
				return 0, err
			}
		}
	}
}

func (r *lzma2Reader) openChunk() error {
	ctrl, err := r.br.ReadByte()
	if err != nil {
		return err
	}
	if ctrl == 0x00 {
		r.state = 2
		return io.EOF
	}
	if ctrl == 0x01 || ctrl == 0x02 {
		r.lit = true
		if ctrl == 0x01 {
			r.d.reset()
		}
		var sz [2]byte
		if _, err := io.ReadFull(r.br, sz[:]); err != nil {
			return err
		}
		r.remain = int(binary.BigEndian.Uint16(sz[:])) + 1
		r.state = 1
		return nil
	}
	if ctrl < 0x80 {
		return fmt.Errorf("xz: invalid lzma2 control %#x", ctrl)
	}
	r.lit = false
	mode := (ctrl >> 5) & 0x03
	uHigh := int(ctrl & 0x1F)
	var b1, b2 byte
	if b1, err = r.br.ReadByte(); err != nil {
		return err
	}
	if b2, err = r.br.ReadByte(); err != nil {
		return err
	}
	uSize := (uHigh<<16 | int(b1)<<8 | int(b2)) + 1

	var c1, c2 byte
	if c1, err = r.br.ReadByte(); err != nil {
		return err
	}
	if c2, err = r.br.ReadByte(); err != nil {
		return err
	}
	cSize := (int(c1)<<8 | int(c2)) + 1

	switch mode {
	case 3:
		props, err := r.br.ReadByte()
		if err != nil {
			return err
		}
		if err := r.dec.setProps(props); err != nil {
			return err
		}
		r.d.reset()
		r.dec.resetDict()
	case 2:
		props, err := r.br.ReadByte()
		if err != nil {
			return err
		}
		if err := r.dec.setProps(props); err != nil {
			return err
		}
	case 1:
		r.dec.resetState()
	case 0:
		if !r.dec.propsSet {
			return errors.New("xz: lzma2 first chunk must reset properties")
		}
	}

	r.remain = uSize
	if err := r.dec.startChunk(r.br, cSize, uSize); err != nil {
		return err
	}
	r.state = 1
	return nil
}

func (r *lzma2Reader) fillChunk() error {
	if r.lit {
		n := r.remain
		if n > 4096 {
			n = 4096
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return err
		}
		r.d.write(buf)
		r.dec.history = append(r.dec.history, buf...)
		r.remain -= n
		if r.remain == 0 {
			r.state = 0
		}
		return nil
	}
	if err := r.dec.decode(); err != nil {
		return err
	}
	r.state = 0
	return nil
}

// dict is the LZ77 window that LZMA writes through.
type dict struct {
	buf []byte
}

func (d *dict) reset()      { d.buf = d.buf[:0] }
func (d *dict) has() bool   { return len(d.buf) > 0 }
func (d *dict) write(p []byte) {
	d.buf = append(d.buf, p...)
}

func (d *dict) read(p []byte) int {
	n := copy(p, d.buf)
	d.buf = d.buf[n:]
	return n
}

// putByte pushes a literal byte into the window.
func (d *dict) putByte(b byte) { d.buf = append(d.buf, b) }

// repeat copies length bytes starting from `dist+1` bytes back in the
// virtual window (which is the previously emitted output).
func (d *dict) repeat(dist, length int, history *[]byte) error {
	src := *history
	off := len(src) - dist - 1
	if off < 0 {
		return errors.New("xz: bad distance")
	}
	for i := 0; i < length; i++ {
		b := src[off+i]
		d.buf = append(d.buf, b)
		src = append(src, b)
		*history = src
	}
	return nil
}

// ------------------------------------------------------------------
// LZMA decoder
// ------------------------------------------------------------------

const (
	kNumStates       = 12
	kNumLenToPosStates = 4
	kNumAlignBits    = 4
	kAlignTableSize  = 1 << kNumAlignBits
	kStartPosModelIdx = 4
	kEndPosModelIdx   = 14
	kNumFullDistances = 1 << (kEndPosModelIdx >> 1)

	kNumPosBitsMax = 4
	kNumPosStatesMax = 1 << kNumPosBitsMax

	kNumLowLenBits  = 3
	kNumMidLenBits  = 3
	kNumHighLenBits = 8
	kNumLowLenSymbols  = 1 << kNumLowLenBits
	kNumMidLenSymbols  = 1 << kNumMidLenBits

	kBitModelTotal = 1 << 11
	kNumMoveBits   = 5
	kNumBitModelTotalBits = 11
	kTopValue = 1 << 24
)

type lzmaDecoder struct {
	d        *dict
	propsSet bool

	lc, lp, pb int

	// State
	state                                            int
	rep0, rep1, rep2, rep3                           int
	isMatch                                          [kNumStates << kNumPosBitsMax]uint16
	isRep, isRepG0, isRepG1, isRepG2                 [kNumStates]uint16
	isRep0Long                                       [kNumStates << kNumPosBitsMax]uint16
	posSlot                                          [kNumLenToPosStates][1 << 6]uint16
	posDecoders                                      [kNumFullDistances - kEndPosModelIdx]uint16
	alignDecoders                                    [kAlignTableSize]uint16
	lenCoder                                         lenCoder
	repLenCoder                                      lenCoder
	litDecoders                                      []uint16

	// Range coder
	rangeV uint32
	code   uint32
	r      io.Reader

	// History — the cumulative output from this LZMA stream so far,
	// used as the LZ77 source. Gets a fresh slice at each dictionary
	// reset.
	history []byte

	cBytesLeft int // compressed bytes left in current chunk
	uLeft      int // uncompressed bytes left to emit in current chunk
}

type lenCoder struct {
	choice  uint16
	choice2 uint16
	low     [kNumPosStatesMax][1 << kNumLowLenBits]uint16
	mid     [kNumPosStatesMax][1 << kNumMidLenBits]uint16
	high    [1 << kNumHighLenBits]uint16
}

func initProbs(p []uint16) {
	for i := range p {
		p[i] = kBitModelTotal / 2
	}
}

func (l *lenCoder) init() {
	l.choice = kBitModelTotal / 2
	l.choice2 = kBitModelTotal / 2
	for i := range l.low {
		initProbs(l.low[i][:])
		initProbs(l.mid[i][:])
	}
	initProbs(l.high[:])
}

func (l *lzmaDecoder) setProps(b byte) error {
	if b > 224 {
		return fmt.Errorf("xz: invalid lzma props %d", b)
	}
	l.lc = int(b % 9)
	rem := int(b / 9)
	l.lp = rem % 5
	l.pb = rem / 5
	if l.lc+l.lp > 4 {
		return errors.New("xz: lc+lp>4 unsupported")
	}
	l.propsSet = true
	l.resetState()
	return nil
}

func (l *lzmaDecoder) resetState() {
	l.state = 0
	l.rep0, l.rep1, l.rep2, l.rep3 = 0, 0, 0, 0
	initProbs(l.isMatch[:])
	initProbs(l.isRep[:])
	initProbs(l.isRepG0[:])
	initProbs(l.isRepG1[:])
	initProbs(l.isRepG2[:])
	initProbs(l.isRep0Long[:])
	for i := range l.posSlot {
		initProbs(l.posSlot[i][:])
	}
	initProbs(l.posDecoders[:])
	initProbs(l.alignDecoders[:])
	l.lenCoder.init()
	l.repLenCoder.init()
	n := 0x300 << uint(l.lc+l.lp)
	if cap(l.litDecoders) < n {
		l.litDecoders = make([]uint16, n)
	} else {
		l.litDecoders = l.litDecoders[:n]
	}
	initProbs(l.litDecoders)
}

func (l *lzmaDecoder) resetDict() {
	l.history = l.history[:0]
}

func (l *lzmaDecoder) startChunk(r io.Reader, cSize, uSize int) error {
	l.r = r
	l.cBytesLeft = cSize
	l.uLeft = uSize

	var first [5]byte
	if _, err := io.ReadFull(l.r, first[:]); err != nil {
		return err
	}
	if first[0] != 0 {
		return errors.New("xz: bad lzma initial byte")
	}
	l.code = binary.BigEndian.Uint32(first[1:5])
	l.rangeV = 0xFFFFFFFF
	l.cBytesLeft -= 5
	return nil
}

func (l *lzmaDecoder) readByte() (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(l.r, b[:]); err != nil {
		return 0, err
	}
	l.cBytesLeft--
	return b[0], nil
}

func (l *lzmaDecoder) normalize() error {
	if l.rangeV < kTopValue {
		b, err := l.readByte()
		if err != nil {
			return err
		}
		l.rangeV <<= 8
		l.code = (l.code << 8) | uint32(b)
	}
	return nil
}

func (l *lzmaDecoder) decodeBit(prob *uint16) (int, error) {
	if err := l.normalize(); err != nil {
		return 0, err
	}
	bound := (l.rangeV >> kNumBitModelTotalBits) * uint32(*prob)
	if l.code < bound {
		l.rangeV = bound
		*prob += (kBitModelTotal - *prob) >> kNumMoveBits
		return 0, nil
	}
	l.rangeV -= bound
	l.code -= bound
	*prob -= *prob >> kNumMoveBits
	return 1, nil
}

func (l *lzmaDecoder) decodeBitTree(probs []uint16, n int) (int, error) {
	idx := 1
	for i := 0; i < n; i++ {
		bit, err := l.decodeBit(&probs[idx])
		if err != nil {
			return 0, err
		}
		idx = (idx << 1) | bit
	}
	return idx - (1 << n), nil
}

func (l *lzmaDecoder) decodeReverseBitTree(probs []uint16, n int) (int, error) {
	idx := 1
	res := 0
	for i := 0; i < n; i++ {
		bit, err := l.decodeBit(&probs[idx])
		if err != nil {
			return 0, err
		}
		idx = (idx << 1) | bit
		res |= bit << i
	}
	return res, nil
}

func (l *lzmaDecoder) decodeDirectBits(n int) (int, error) {
	res := 0
	for i := 0; i < n; i++ {
		if err := l.normalize(); err != nil {
			return 0, err
		}
		l.rangeV >>= 1
		t := (l.code - l.rangeV) >> 31
		l.code -= l.rangeV & (t - 1)
		res = (res << 1) | int(1-t)
	}
	return res, nil
}

func (l *lzmaDecoder) decodeLen(coder *lenCoder, posState int) (int, error) {
	bit, err := l.decodeBit(&coder.choice)
	if err != nil {
		return 0, err
	}
	if bit == 0 {
		v, err := l.decodeBitTree(coder.low[posState][:], kNumLowLenBits)
		return 2 + v, err
	}
	bit2, err := l.decodeBit(&coder.choice2)
	if err != nil {
		return 0, err
	}
	if bit2 == 0 {
		v, err := l.decodeBitTree(coder.mid[posState][:], kNumMidLenBits)
		return 2 + kNumLowLenSymbols + v, err
	}
	v, err := l.decodeBitTree(coder.high[:], kNumHighLenBits)
	return 2 + kNumLowLenSymbols + kNumMidLenSymbols + v, err
}

func (l *lzmaDecoder) decode() error {
	for l.uLeft > 0 {
		posState := len(l.history) & ((1 << l.pb) - 1)
		matchIdx := (l.state << kNumPosBitsMax) + posState
		bit, err := l.decodeBit(&l.isMatch[matchIdx])
		if err != nil {
			return err
		}
		if bit == 0 {
			if err := l.decodeLiteral(); err != nil {
				return err
			}
			l.uLeft--
			continue
		}
		var matchLen int
		bit, err = l.decodeBit(&l.isRep[l.state])
		if err != nil {
			return err
		}
		if bit != 0 {
			matchLen, err = l.decodeRep(posState)
			if err != nil {
				return err
			}
		} else {
			l.rep3 = l.rep2
			l.rep2 = l.rep1
			l.rep1 = l.rep0
			matchLen, err = l.decodeLen(&l.lenCoder, posState)
			if err != nil {
				return err
			}
			l.state = stateUpdateMatch(l.state)
			if err := l.decodeDistance(matchLen); err != nil {
				return err
			}
		}
		if matchLen > l.uLeft {
			return errors.New("xz: lzma overshoot uncompressed size")
		}
		if err := l.d.repeat(l.rep0, matchLen, &l.history); err != nil {
			return err
		}
		l.uLeft -= matchLen
	}
	for l.cBytesLeft > 0 {
		if _, err := l.readByte(); err != nil {
			return err
		}
	}
	if l.cBytesLeft != 0 {
		return errors.New("xz: lzma chunk size mismatch")
	}
	return nil
}

func stateUpdateLit(s int) int {
	switch {
	case s < 4:
		return 0
	case s < 10:
		return s - 3
	default:
		return s - 6
	}
}

func stateUpdateMatch(s int) int {
	if s < 7 {
		return 7
	}
	return 10
}

func stateUpdateRep(s int) int {
	if s < 7 {
		return 8
	}
	return 11
}

func stateUpdateShortRep(s int) int {
	if s < 7 {
		return 9
	}
	return 11
}

func (l *lzmaDecoder) decodeLiteral() error {
	var prevByte byte
	if len(l.history) > 0 {
		prevByte = l.history[len(l.history)-1]
	}
	pos := len(l.history)
	litState := ((pos & ((1 << l.lp) - 1)) << l.lc) | int(prevByte>>(8-uint(l.lc)))
	probs := l.litDecoders[0x300*litState:]
	symbol := 1
	if l.state >= 7 {
		// match literal: combine with the byte at distance rep0+1 back
		matchByte := byte(0)
		off := len(l.history) - l.rep0 - 1
		if off >= 0 {
			matchByte = l.history[off]
		} else {
			return errors.New("xz: bad match-literal offset")
		}
		for symbol < 0x100 {
			matchBit := int(matchByte>>7) & 1
			matchByte <<= 1
			bit, err := l.decodeBit(&probs[((1+matchBit)<<8)+symbol])
			if err != nil {
				return err
			}
			symbol = (symbol << 1) | bit
			if matchBit != bit {
				break
			}
		}
	}
	for symbol < 0x100 {
		bit, err := l.decodeBit(&probs[symbol])
		if err != nil {
			return err
		}
		symbol = (symbol << 1) | bit
	}
	b := byte(symbol & 0xFF)
	l.d.putByte(b)
	l.history = append(l.history, b)
	l.state = stateUpdateLit(l.state)
	return nil
}

func (l *lzmaDecoder) decodeRep(posState int) (int, error) {
	bit, err := l.decodeBit(&l.isRepG0[l.state])
	if err != nil {
		return 0, err
	}
	if bit == 0 {
		idx := (l.state << kNumPosBitsMax) + posState
		bit2, err := l.decodeBit(&l.isRep0Long[idx])
		if err != nil {
			return 0, err
		}
		if bit2 == 0 {
			// short rep: copy a single byte at rep0
			off := len(l.history) - l.rep0 - 1
			if off < 0 {
				return 0, errors.New("xz: short rep with empty history")
			}
			b := l.history[off]
			l.d.putByte(b)
			l.history = append(l.history, b)
			l.state = stateUpdateShortRep(l.state)
			l.uLeft--
			return 0, nil
		}
	} else {
		bit2, err := l.decodeBit(&l.isRepG1[l.state])
		if err != nil {
			return 0, err
		}
		var dist int
		if bit2 == 0 {
			dist = l.rep1
		} else {
			bit3, err := l.decodeBit(&l.isRepG2[l.state])
			if err != nil {
				return 0, err
			}
			if bit3 == 0 {
				dist = l.rep2
			} else {
				dist = l.rep3
				l.rep3 = l.rep2
			}
			l.rep2 = l.rep1
		}
		l.rep1 = l.rep0
		l.rep0 = dist
	}
	matchLen, err := l.decodeLen(&l.repLenCoder, posState)
	if err != nil {
		return 0, err
	}
	l.state = stateUpdateRep(l.state)
	return matchLen, nil
}

func (l *lzmaDecoder) decodeDistance(matchLen int) error {
	lenState := matchLen - 2
	if lenState >= kNumLenToPosStates {
		lenState = kNumLenToPosStates - 1
	}
	posSlot, err := l.decodeBitTree(l.posSlot[lenState][:], 6)
	if err != nil {
		return err
	}
	if posSlot < 4 {
		l.rep0 = posSlot
		return nil
	}
	numDirect := (posSlot >> 1) - 1
	base := (2 | (posSlot & 1)) << numDirect
	if posSlot < kEndPosModelIdx {
		v, err := l.decodeReverseBitTreeAt(l.posDecoders[:], base-posSlot-1, numDirect)
		if err != nil {
			return err
		}
		l.rep0 = base | v
		return nil
	}
	d, err := l.decodeDirectBits(numDirect - kNumAlignBits)
	if err != nil {
		return err
	}
	d <<= kNumAlignBits
	a, err := l.decodeReverseBitTree(l.alignDecoders[:], kNumAlignBits)
	if err != nil {
		return err
	}
	l.rep0 = base | d | a
	return nil
}

// decodeReverseBitTreeAt walks a reverse bit tree whose virtual root
// is at index 1 of `probs[base:]`. base may be -1, the canonical LZMA
// trick that lets the per-slot subtrees pack tightly into a shared
// posDecoders array.
func (l *lzmaDecoder) decodeReverseBitTreeAt(probs []uint16, base, n int) (int, error) {
	idx := 1
	res := 0
	for i := 0; i < n; i++ {
		bit, err := l.decodeBit(&probs[base+idx])
		if err != nil {
			return 0, err
		}
		idx = (idx << 1) | bit
		res |= bit << i
	}
	return res, nil
}
