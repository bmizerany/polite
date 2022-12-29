package febeio

import (
	"bufio"
	"errors"
	"io"

	"go4.org/mem"
)

// Conn reads and writes postgres wire protocol message types to and from it's
// underlying Reader and Writers, without allocating.
//
// It is not safe for concurrent use.
//
// See: https://www.postgresql.org/docs/current/protocol-message-types.html for
// more information about the postgres message data types.
type Conn struct {
	r *bufio.Reader
	w *bufio.Writer

	fr fullReader
	rr io.Reader
}

// NewConn returns a Conn ready for use.
//
// The provided reader and writer will be wrapped in a bufio.Reader and
// bufio.Writer via bufio.NewReaderSize and bufio.NewWriterSize. This means
// clients may use a buffer size larger than the default by passing
// bufio.Reader and bufio.Writer.
func NewConn(r io.Reader, w io.Writer) *Conn {
	c := &Conn{
		r: bufio.NewReader(r),
		w: bufio.NewWriter(w),
	}
	c.rr = &c.fr
	return c
}

// ReadByte reads a single byte from the stream.
func (c *Conn) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return b, nil
}

func (c *Conn) WriteByte(b byte) error {
	return c.w.WriteByte(b)
}

func (c *Conn) ReadInt16() (int16, error) {
	buf, err := c.r.Peek(2)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	c.r.Discard(len(buf))
	n := int16(buf[0])<<8 | int16(buf[1])
	return n, nil
}

func (c *Conn) WriteInt16(n int16) error {
	c.w.WriteByte(byte(n >> 8))
	return c.w.WriteByte(byte(n))
}

func (c *Conn) ReadInt32() (int32, error) {
	buf, err := c.r.Peek(4)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	c.r.Discard(len(buf))
	n := int32(buf[0])<<24 | int32(buf[1])<<16 | int32(buf[2])<<8 | int32(buf[3])
	return n, nil
}

func (c *Conn) WriteInt32(n int32) error {
	c.w.WriteByte(byte(n >> 24))
	c.w.WriteByte(byte(n >> 16))
	c.w.WriteByte(byte(n >> 8))
	return c.w.WriteByte(byte(n))
}

type connReadCloser struct {
	*Conn
}

func (c connReadCloser) Read(p []byte) (int, error) {
	return c.rr.Read(p)
}

func (c connReadCloser) Close() error {
	return c.discardRemainder()
}

// ReadByteN returns a reader that reads upto n bytes from the stream. If a
// call to the underlying reader's Read results in io.EOF before n bytes have
// been read, then io.ErrUnexpectedEOF is returned.
func (c *Conn) ReadByteN(n int) io.ReadCloser {
	c.fr.R = c.r
	c.fr.N = n
	return connReadCloser{c}
}

func (c *Conn) discardRemainder() error {
	_, err := c.r.Discard(c.fr.N)

	// make calling this method again a nop if called before another call
	// to ReadByteN.
	c.fr.N = 0
	return err
}

// ReadString reads a null terminated string from the stream. The mem.RO is
// valid until the next call to any read method on the Conn.
//
// Errors received from the underlying reader are returned as is. As an
// exception, io.ErrUnexpectedEOF is returned along with all bytes read, if the
// underlying reader returns EOF before a null byte is encountered.
//
// ReadString does not continue looking for a null byte after encountering an
// error.
func (c *Conn) ReadString() (mem.RO, error) {
	b, err := c.r.ReadSlice(0)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return mem.B(b), err
	}
	return mem.B(b[:len(b)-1]), nil
}

func (c *Conn) WriteString(s mem.RO) error {
	_, err := io.Copy(c.w, mem.NewReader(s))
	c.w.WriteByte(0)
	return err
}

// Flush calls the underlying bufio.Writer's Flush method.
func (c *Conn) Flush() error {
	return c.w.Flush()
}

// fullReader is an io.Reader that reads upto N bytes from R. If the underlying
// reader returns io.EOF before N bytes have been read, Read returns
// io.ErrUnexpectedEOF.
type fullReader struct {
	R io.Reader
	N int
}

func (r *fullReader) Read(p []byte) (int, error) {
	if r.N <= 0 {
		return 0, io.EOF
	}
	if len(p) > r.N {
		p = p[:r.N]
	}
	n, err := r.R.Read(p)
	r.N -= n
	if errors.Is(err, io.EOF) && r.N > 0 {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}
