package febe

import (
	"context"
	"errors"
	"io"
	"sync"

	"blake.io/pqlite/febe/febeio"
	"go4.org/mem"
)

type Conn struct {
	mu sync.Mutex
	c  *febeio.Conn
}

func New(r io.Reader, w io.Writer) *Conn {
	c := febeio.NewConn(r, w)
	return &Conn{c: c}
}

func (c *Conn) Flush() error {
	return c.c.Flush()
}

func (c *Conn) Err() error {
	return c.c.Err()
}

func (c *Conn) WriteStartupMessage(ctx context.Context, params ...string) (err error) {
	if len(params)%2 != 0 {
		return errors.New("params must be even")
	}

	var s sizer
	s.addInt32() // length
	s.addInt32() // protocol version
	s.addStrings(params...)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.c.WriteInt32(s.size())
	c.c.WriteInt32(196608)
	for i := 0; i < len(params); i += 2 {
		c.c.WriteString(mem.S(params[i]))
		c.c.WriteString(mem.S(params[i+1]))
	}
	return c.Err()
}

func (c *Conn) ReadStartupResponse() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := c.c.ReadByte()
	if err != nil {
		return err
	}
	if b != 'R' {
		return errors.New("expected 'R'")
	}
	c.c.ReadInt32()           // length
	t, err := c.c.ReadInt32() // protocol version
	if err != nil {
		return err
	}
	if t != 0 {
		// https://www.postgresql.org/docs/current/protocol-message-formats.html
		return errors.New("unexpected autentication response")
	}
	return nil
}

type sizer int32

func (s *sizer) addInt32()          { *s += 4 }
func (s *sizer) addInt16()          { *s += 2 }
func (s *sizer) addBytes(v []byte)  { *s += sizer(len(v)) }
func (s *sizer) addString(v string) { *s += sizer(len(v) + 1) }
func (s *sizer) addStrings(vv ...string) {
	for _, v := range vv {
		s.addString(v)
	}
}
func (s *sizer) size() int32 { return int32(*s) }
