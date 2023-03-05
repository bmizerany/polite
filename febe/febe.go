package febe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"blake.io/pqlite/febe/febeio"
	"go4.org/mem"
)

func checkMessageHeader(h MessageHeader, want byte) error {
	if h.Type() != want {
		return fmt.Errorf("pqlite: febe: unexpected message type: %q want %q", h.Type(), want)
	}
	return nil
}

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

type AuthMessage struct {
	MessageHeader
	Type int32
}

func (m AuthMessage) Ok() bool        { return m.Type == 0 }
func (m AuthMessage) Cleartext() bool { return m.Type == 3 }

func (c *Conn) ReadAuthentiationMessage(h MessageHeader) (AuthMessage, error) {
	if err := checkMessageHeader(h, 'R'); err != nil {
		return AuthMessage{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t, err := c.c.ReadInt32()
	if err != nil {
		return AuthMessage{}, err
	}
	return AuthMessage{MessageHeader: h, Type: t}, nil
}

type Message interface {
	Header() *MessageHeader
}

type MessageHeader struct {
	msgType byte
	msgLen  int
}

func (h MessageHeader) Type() byte            { return h.msgType }
func (h MessageHeader) Len() int              { return h.msgLen }
func (h MessageHeader) Header() MessageHeader { return h }

func (c *Conn) ReadHeader() (MessageHeader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := c.c.ReadByte()
	if err != nil {
		return MessageHeader{}, err
	}
	l, err := c.c.ReadInt32()
	if err != nil {
		return MessageHeader{}, err
	}
	h := MessageHeader{msgType: b, msgLen: int(l)}
	return h, nil
}

// TODO(bmizerany): https://www.postgresql.org/docs/current/errcodes-appendix.html

type sizer int32

func (s *sizer) addInt32()          { *s += 4 }
func (s *sizer) addInt16()          { *s += 2 }
func (s *sizer) addByte()           { *s += 1 }
func (s *sizer) addBytes(v []byte)  { *s += sizer(len(v)) }
func (s *sizer) addString(v string) { *s += sizer(len(v) + 1) }
func (s *sizer) addStrings(vv ...string) {
	for _, v := range vv {
		s.addString(v)
	}
}
func (s *sizer) size() int32 { return int32(*s) }
