package febeio

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

const postgresBufferSize = 8 << 10

// Conn reads and writes postgres wire protocol message types to and from it's
// underlying Reader and Writers, without allocating.
//
// It is not safe for concurrent use.
//
// See: https://www.postgresql.org/docs/current/protocol-message-types.html for
// more information about the postgres message data types.
type Conn struct {
	rw *setErrReadWriter
	r *byteReader
	w *bufio.Writer
	n int // number of bytes read

	err error
}

type setErrReadWriter struct {
	r io.Reader
	w io.Writer
}

func (rw *setErrReadWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *setErrReadWriter) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

// NewConn returns a Conn ready for use.
//
// The provided reader and writer will be wrapped in a bufio.Reader and
// bufio.Writer via bufio.NewReaderSize and bufio.NewWriterSize. This means
// clients may use a buffer size larger than the default by passing
// bufio.Reader and bufio.Writer.
func NewConn(r io.Reader, w io.Writer) *Conn {
	rw := &setErrReadWriter{r: r, w: w}
	cn := &Conn{rw: rw}
	cn.r = &byteReader{r: rw}
	cn.w = bufio.NewWriterSize(rw, postgresBufferSize)
	return cn
}

func (c *Conn) N() int  { return c.n }
func (c *Conn) ResetN() { c.n = 0 }

func isTimeout(err error) bool {
	type timeout interface {
		Timeout() bool
	}
	t, ok := err.(timeout)
	return ok && t.Timeout()
}

func (c *Conn) setErr(err error) error {
	if err == nil {
		return nil
	}
	if c.err != nil {
		return c.err
	}
	if isTimeout(err) {
		return err
	}
	c.err = err
	return c.err
}

func (c *Conn) Err() error {
	if c.err != nil {
		return c.err
	}
	_, err := c.w.Write(nil)
	if err != nil {
		if isTimeout(err) {
			c.w.Reset(c.rw)
		}
		return c.setErr(err)
	}
	return nil
}

func (c *Conn) peek(n int) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	w := c.r.window()
	for len(w) < n {
		_, err := c.r.extend()
		if err != nil {
			if errors.Is(err, io.EOF) && len(c.r.window()) < n {
				return nil, c.setErr(io.ErrUnexpectedEOF)
			}
			if len(c.r.window()) < n {
				return nil, c.setErr(err)
			}
		}
		w = c.r.window()
	}
	c.releaseAndCount(n)
	return w[:n], nil
}

func (c *Conn) releaseAndCount(n int) {
	c.r.release(n)
	c.n += n
}

// ReadByte reads a single byte from the stream.
func (c *Conn) ReadByte() (byte, error) {
	data, err := c.peek(1)
	if err != nil {
		return 0, err
	}
	return data[0], nil
}

func (c *Conn) WriteByte(b byte) error {
	return c.w.WriteByte(b)
}

func (c *Conn) ReadInt16() (int16, error) {
	b, err := c.peek(2)
	if err != nil {
		return 0, err
	}
	n := int16(b[0])<<8 | int16(b[1])
	return n, nil
}

func (c *Conn) WriteInt16(n int16) error {
	c.w.WriteByte(byte(n >> 8))
	return c.w.WriteByte(byte(n))
}

func (c *Conn) ReadInt32() (int32, error) {
	// TODO(bmizerany): use ctx to SetDeadline
	b, err := c.peek(4)
	if err != nil {
		return 0, err
	}
	n := int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3])
	return n, nil
}

func (c *Conn) WriteInt32(n int32) error {
	c.w.WriteByte(byte(n >> 24))
	c.w.WriteByte(byte(n >> 16))
	c.w.WriteByte(byte(n >> 8))
	return c.w.WriteByte(byte(n))
}

// ReadByteN returns upto n bytes from the stream, or an error if fewer than
// n bytes are available.
func (c *Conn) ReadByteN(n int) ([]byte, error) {
	return c.peek(n)
}

func (c *Conn) WriteBytesString(s string) error {
	if err := c.WriteInt32(int32(len(s))); err != nil {
		return err
	}
	io.WriteString(c.w, s)
	return c.Err()
}

func (c *Conn) WriteString(s string) error {
	io.WriteString(c.w, s)
	return c.w.WriteByte(0)
}

func (c *Conn) WriteSizedBytes(b []byte) error {
	if err := c.WriteInt32(int32(len(b))); err != nil {
		return err
	}
	c.w.Write(b)
	return c.Err()
}

// Write writes b to the underlying writer.
func (c *Conn) Write(b []byte) (int, error) {
	return c.w.Write(b)
}

func (c *Conn) ReadBytes() ([]byte, error) {
	n, err := c.ReadInt32()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, errors.New("invalid message length")
	}
	return c.ReadByteN(int(n))
}

// ReadString reads a null terminated string from the stream. The byte slice is
// valid until the next call to any read method on the Conn.
//
// Errors received from the underlying reader are returned as is. As an
// exception, io.ErrUnexpectedEOF is returned along with all bytes read, if the
// underlying reader returns EOF before a null byte is encountered.
//
// ReadString does not continue looking for a null byte after encountering an
// error.
func (c *Conn) ReadString() ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	w := c.r.window()
	for {
		// TODO(bmizerany): track position in window to avoid
		// re-scanning bytes when there is no terminating null byte
		// in a previous pass.
		i := bytes.IndexByte(w, 0)
		if i >= 0 {
			c.releaseAndCount(i + 1)
			return w[:i], nil
		}
		_, err := c.r.extend()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, c.setErr(io.ErrUnexpectedEOF)
			}
		}
		w = c.r.window()
		if err != nil && bytes.IndexByte(w, 0) < 0 {
			return nil, c.setErr(err)
		}
	}
}

func (c *Conn) Discard(n int) error {
	if c.err != nil {
		return c.err
	}
	w := c.r.window()
	for {
		rel := min(n, len(w))
		c.releaseAndCount(rel)
		n -= rel
		if n <= 0 {
			return nil
		}
		_, err := c.r.extend()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return c.setErr(io.ErrUnexpectedEOF)
			}
			return c.setErr(err)
		}
		w = c.r.window()
	}
}

// Flush calls the underlying bufio.Writer's Flush method.
func (c *Conn) Flush() error {
	if c.err != nil {
		return c.err
	}
	err := c.w.Flush()
	if err != nil && isTimeout(err) {
		c.w.Reset(c.rw)
	}
	return c.setErr(err)
}
