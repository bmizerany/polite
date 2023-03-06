// Package febe implements the encoding and decoding of Frontend/Backend
// (FE/BE) messages.
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

type ReadyStatus byte

const (
	ReadyStatusIdle   ReadyStatus = 'I'
	ReadyStatusBusy   ReadyStatus = 'T'
	ReadyStatusFailed ReadyStatus = 'E'
)

type ReadyForQueryResponse struct {
	MessageHeader
	Status ReadyStatus
}

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

type Message interface {
	Header() MessageHeader
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

type AuthenticationType int32

const (
	AuthenticationTypeOK        AuthenticationType = 0
	AuthenticationTypeCleartext AuthenticationType = 3
)

type AuthenticationResponse struct {
	MessageHeader
	Type AuthenticationType
}

func (c *Conn) ReadAuthentication(ctx context.Context, h MessageHeader) (AuthenticationResponse, error) {
	if err := checkMessageHeader(h, 'R'); err != nil {
		return AuthenticationResponse{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t, err := c.c.ReadInt32()
	if err != nil {
		return AuthenticationResponse{}, err
	}
	return AuthenticationResponse{MessageHeader: h, Type: AuthenticationType(t)}, nil
}

func (c *Conn) WriteAuthentication(ctx context.Context, typ AuthenticationType) error {
	var s sizer
	s.addInt32() // length
	s.addInt32() // type

	c.mu.Lock()
	defer c.mu.Unlock()
	c.c.WriteByte('R')
	c.c.WriteInt32(s.size())
	c.c.WriteInt32(int32(typ))
	return c.Err()
}

func (c *Conn) WriteBackendKeyData(ctx context.Context, pid, secret int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.c.WriteByte('K')
	c.c.WriteInt32(12)
	c.c.WriteInt32(pid)
	c.c.WriteInt32(secret)
	return c.Err()
}

func (c *Conn) WriteParameterStatus(ctx context.Context, name, value string) error {
	var s sizer
	s.addInt32() // length
	s.addStrings(name, value)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.c.WriteByte('S')
	c.c.WriteInt32(s.size())
	c.c.WriteString(mem.S(name))
	c.c.WriteString(mem.S(value))
	return c.Err()
}

func (c *Conn) WriteReadyForQuery(ctx context.Context, status ReadyStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.c.WriteByte('Z')
	c.c.WriteInt32(5)
	c.c.WriteByte(byte(status))
	return c.Err()
}

func (c *Conn) ReadReadyForQuery(ctx context.Context, h MessageHeader) (ReadyForQueryResponse, error) {
	if err := checkMessageHeader(h, 'Z'); err != nil {
		return ReadyForQueryResponse{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := c.c.ReadByte()
	if err != nil {
		return ReadyForQueryResponse{}, err
	}
	r := ReadyForQueryResponse{MessageHeader: h, Status: ReadyStatus(b)}
	return r, nil
}

func (c *Conn) WriteCommandComplete(ctx context.Context, tag string) error {
	var s sizer
	s.addInt32() // length
	s.addStrings(tag)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.c.WriteByte('C')
	c.c.WriteInt32(s.size())
	c.c.WriteString(mem.S(tag))
	return c.Err()
}

type CommandCompleteResponse struct {
	MessageHeader
	Tag string
}

func (c *Conn) ReadCommandComplete(ctx context.Context, h MessageHeader) (CommandCompleteResponse, error) {
	if err := checkMessageHeader(h, 'C'); err != nil {
		return CommandCompleteResponse{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t, err := c.c.ReadString()
	if err != nil {
		return CommandCompleteResponse{}, err
	}
	return CommandCompleteResponse{MessageHeader: h, Tag: t.StringCopy()}, nil
}

func (c *Conn) WriteDataRowHeader(ctx context.Context, n int16) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.c.WriteByte('D')
	c.c.WriteInt32(6)
	c.c.WriteInt16(n)
	return c.Err()
}

type DataRowHeaderResponse struct {
	MessageHeader
	N int16
}

func (c *Conn) ReadDataRowHeader(ctx context.Context, h MessageHeader) (DataRowHeaderResponse, error) {
	if err := checkMessageHeader(h, 'D'); err != nil {
		return DataRowHeaderResponse{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.c.ReadInt16()
	if err != nil {
		return DataRowHeaderResponse{}, err
	}
	return DataRowHeaderResponse{MessageHeader: h, N: n}, nil
}
func (c *Conn) WriteDataRowValue(ctx context.Context, v mem.RO) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.c.WriteInt32(int32(v.Len()))
	c.c.WriteByteN(v)
	return c.Err()
}

func (c *Conn) ReadDataRowValue(ctx context.Context, h MessageHeader) (io.ReadCloser, error) {
	if err := checkMessageHeader(h, 'D'); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	l, err := c.c.ReadInt32()
	if err != nil {
		return nil, err
	}
	return c.c.ReadByteN(int(l)), nil
}
