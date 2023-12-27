package febeio

import (
	"bufio"
	"bytes"
	"context"
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

// Extend adds the next message to the underlying buffer so subsequent calls
// to ReadX methods for processing the message will not result in blocking
// reads.
//
// It does not advance the offset of the underlying buffer.
func (c *Conn) Extend(ctx context.Context) error {
	// TODO(bmizerany): use ctx to SetDeadline
	data, err := c.lookAhead(5)
	if err != nil {
		return err
	}
	msgLen := int32(data[1])<<24 | int32(data[2])<<16 | int32(data[3])<<8 | int32(data[4])
	if msgLen < 4 {
		return errors.New("invalid message length")
	}
	if _, err := c.lookAhead(int(msgLen) + 4); err != nil {
		return err
	}
	return nil
}

func (c *Conn) lookAhead(n int) ([]byte, error) {
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
	data, err := c.ReadByteN(1)
	if err != nil {
		return 0, err
	}
	return data[0], nil
}

func (c *Conn) WriteByte(b byte) error {
	return c.w.WriteByte(b)
}

func (c *Conn) ReadInt16() (int16, error) {
	b, err := c.ReadByteN(2)
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
	b, err := c.ReadByteN(4)
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

// ReadByteN returns a reader that reads upto n bytes from the stream. If a
// call to the underlying reader's Read results in io.EOF before n bytes have
// been read, then io.ErrUnexpectedEOF is returned.
func (c *Conn) ReadByteN(n int) ([]byte, error) {
	w, err := c.lookAhead(n)
	if err != nil {
		return nil, err
	}
	c.r.release(n)
	return w[:n], nil
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
	w := c.r.window()
	for {
		// TODO(bmizerany): track position in window to avoid
		// re-scanning bytes when there is no terminating null byte
		// in a previous pass.
		i := bytes.IndexByte(w, 0)
		if i >= 0 {
			c.r.release(i + 1)
			return w[:i], nil
		}
		if c.r.extend() == 0 {
			return nil, io.ErrUnexpectedEOF
		}
		w = c.r.window()
	}
}

func (c *Conn) Discard(n int) error {
	w := c.r.window()
	for {
		rel := min(n, len(w))
		c.r.release(rel)
		n -= rel
		if n <= 0 {
			return nil
		}
		if c.r.extend() == 0 {
			return io.ErrUnexpectedEOF
		}
		w = c.r.window()
	}
}

// Flush calls the underlying bufio.Writer's Flush method.
func (c *Conn) Flush() error {
	return c.w.Flush()
}
