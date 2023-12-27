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
	r *byteReader
	w *bufio.Writer

	fr fullReader
}

// NewConn returns a Conn ready for use.
//
// The provided reader and writer will be wrapped in a bufio.Reader and
// bufio.Writer via bufio.NewReaderSize and bufio.NewWriterSize. This means
// clients may use a buffer size larger than the default by passing
// bufio.Reader and bufio.Writer.
func NewConn(r io.Reader, w io.Writer) *Conn {
	c := &Conn{
		r: &byteReader{r: r},
		w: bufio.NewWriterSize(w, postgresBufferSize),
	}
	return c
}

func (c *Conn) Err() error {
	_, err := c.w.Write(nil)
	if err != nil {
		return err
	}
	return c.r.err
}

func (c *Conn) peek(n int) ([]byte, error) {
	w := c.r.window()
	for len(w) < n {
		if c.r.extend() == 0 {
			return nil, io.ErrUnexpectedEOF
		}
		w = c.r.window()
	}
	return w[:n], nil
}

// ReadByte reads a single byte from the stream.
func (c *Conn) ReadByte() (byte, error) {
	data, err := c.peek(1)
	if err != nil {
		return 0, err
	}
	b := data[0]
	c.r.release(1)
	return b, nil
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
	c.r.release(2)
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
		return 0, eofAsUnexpectedEOF(err)
	}
	n := int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3])
	c.r.release(4)
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
	r := &c.fr
	return r.Read(p)
}

func (c connReadCloser) Close() error {
	return nil
}

type errReader struct {
	err error
}

func (e *errReader) Read(p []byte) (int, error) {
	return 0, e.err
}

func (e *errReader) Close() error {
	return e.err
}

// ReadByteN returns a reader that reads upto n bytes from the stream. If a
// call to the underlying reader's Read results in io.EOF before n bytes have
// been read, then io.ErrUnexpectedEOF is returned.
func (c *Conn) ReadByteN(n int) io.ReadCloser {
	data, err := c.peek(n)
	if err != nil {
		return &errReader{err}
	}
	c.fr.R = bytes.NewReader(bytes.Clone(data))
	c.fr.N = n
	c.r.release(n)
	return connReadCloser{c}
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

func (c *Conn) WriteBytes(b []byte) error {
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

func (c *Conn) ReadFrom(r io.Reader) (int64, error) {
	return c.w.ReadFrom(r)
}

func (c *Conn) ReadBytes() (io.ReadCloser, error) {
	n, err := c.ReadInt32()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, errors.New("invalid message length")
	}
	return c.ReadByteN(int(n)), nil
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
	w := c.r.window()
	for {
		i := bytes.IndexByte(w, 0)
		if i >= 0 {
			str := bytes.Clone(w[:i])
			c.r.release(len(str) + 1)
			return str, nil
		}
		if c.r.extend() == 0 {
			return nil, io.ErrUnexpectedEOF
		}
		w = c.r.window()
	}
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
	return n, eofAsUnexpectedEOF(err)
}

func eofAsUnexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
