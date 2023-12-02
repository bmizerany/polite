// Package febe implements the encoding and decoding of Frontend/Backend
// (FE/BE) messages.
package febe

import (
	"context"
	"fmt"
	"io"
	"strings"

	"blake.io/pqlite/febe/febeio"
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
	c *febeio.Conn
}

func New(r io.Reader, w io.Writer) *Conn {
	c := febeio.NewConn(r, w)
	return &Conn{c: c}
}

func (c *Conn) Err() error {
	return c.c.Err()
}

func (c *Conn) Flush() error {
	return c.c.Flush()
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
func (h MessageHeader) BodyLen() int          { return h.msgLen - 4 }
func (h MessageHeader) Header() MessageHeader { return h }
func (h MessageHeader) String() string {
	return fmt.Sprintf("<MessageHeader Type:%q Len:%d>", h.msgType, h.msgLen)
}

func (c *Conn) ReadHeader(ctx context.Context) (MessageHeader, error) {
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

func (c *Conn) DiscardMessage(ctx context.Context, h MessageHeader) error {
	return c.c.ReadByteN(h.BodyLen()).Close()
}

// TODO(bmizerany): https://www.postgresql.org/docs/current/errcodes-appendix.html

type sizer int32

func (s *sizer) addInt32()               { *s += 4 }
func (s *sizer) addInt16()               { *s += 2 }
func (s *sizer) addByte()                { *s += 1 }
func (s *sizer) addBytesString(v string) { *s += 4 + sizer(len(v)) }
func (s *sizer) addStrings(vv ...string) {
	for _, v := range vv {
		*s += sizer(len(v) + 1)
	}
}
func (s *sizer) size() int32 { return int32(*s) }

func (c *Conn) WriteStartupMessage(ctx context.Context, params ...string) (err error) {
	var s sizer
	s.addInt32() // length
	s.addInt32() // protocol version
	for i := 0; i < len(params); i++ {
		k, v, _ := strings.Cut(params[i], "=")
		s.addStrings(k)
		s.addStrings(v)
	}
	s.addStrings("")

	c.c.WriteInt32(s.size())
	c.c.WriteInt32(196608)
	for i := 0; i < len(params); i++ {
		k, v, _ := strings.Cut(params[i], "=")
		c.c.WriteString(k)
		c.c.WriteString(v)
	}
	c.c.WriteString("")
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
	var zero AuthenticationResponse
	if err := checkMessageHeader(h, 'R'); err != nil {
		return zero, err
	}
	t, err := c.c.ReadInt32()
	if err != nil {
		return zero, err
	}
	return AuthenticationResponse{
		MessageHeader: h,
		Type:          AuthenticationType(t),
	}, nil
}

func (c *Conn) WriteAuthentication(ctx context.Context, typ AuthenticationType) error {
	var s sizer
	s.addInt32() // length
	s.addInt32() // type

	c.c.WriteByte('R')
	c.c.WriteInt32(s.size())
	c.c.WriteInt32(int32(typ))
	return c.Err()
}

func (c *Conn) WriteBackendKeyData(ctx context.Context, pid, secret int32) error {
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

	c.c.WriteByte('S')
	c.c.WriteInt32(s.size())
	c.c.WriteString(name)
	c.c.WriteString(value)
	return c.Err()
}

func (c *Conn) WriteReadyForQuery(ctx context.Context, status ReadyStatus) error {
	c.c.WriteByte('Z')
	c.c.WriteInt32(5)
	c.c.WriteByte(byte(status))
	return c.Err()
}

func (c *Conn) ReadReadyForQuery(ctx context.Context, h MessageHeader) (ReadyForQueryResponse, error) {
	if err := checkMessageHeader(h, 'Z'); err != nil {
		return ReadyForQueryResponse{}, err
	}
	b, err := c.c.ReadByte()
	if err != nil {
		return ReadyForQueryResponse{}, err
	}
	return ReadyForQueryResponse{
		MessageHeader: h,
		Status:        ReadyStatus(b),
	}, nil
}

type NotificationResponse struct {
	MessageHeader
	PID     int32
	Channel []byte
	Payload []byte
}

func (c *Conn) ReadNotification(ctx context.Context, h MessageHeader) (NotificationResponse, error) {
	if err := checkMessageHeader(h, 'A'); err != nil {
		return NotificationResponse{}, err
	}
	pid, err := c.c.ReadInt32()
	if err != nil {
		return NotificationResponse{}, err
	}
	channel, err := c.c.ReadString()
	if err != nil {
		return NotificationResponse{}, err
	}
	payload, err := c.c.ReadString()
	if err != nil {
		return NotificationResponse{}, err
	}
	return NotificationResponse{
		MessageHeader: h,
		PID:           pid,
		Channel:       channel,
		Payload:       payload,
	}, nil
}

func (c *Conn) WriteQuery(ctx context.Context, query string) error {
	var s sizer
	s.addInt32() // length
	s.addStrings(query)

	c.c.WriteByte('Q')
	c.c.WriteInt32(s.size())
	c.c.WriteString(query)
	return c.Err()
}

func (c *Conn) WriteEmptyQueryResponse(ctx context.Context) error {
	c.c.WriteByte('I')
	c.c.WriteInt32(4)
	return c.Err()
}

type EmptyQueryResponse struct {
	MessageHeader
}

func (c *Conn) ReadEmptyQuery(ctx context.Context, h MessageHeader) (EmptyQueryResponse, error) {
	if err := checkMessageHeader(h, 'I'); err != nil {
		return EmptyQueryResponse{}, err
	}
	return EmptyQueryResponse{MessageHeader: h}, nil
}

func (c *Conn) WriteCommandComplete(ctx context.Context, tag string) error {
	var s sizer
	s.addInt32() // length
	s.addStrings(tag)

	c.c.WriteByte('C')
	c.c.WriteInt32(s.size())
	c.c.WriteString(tag)
	return c.Err()
}

type CommandCompleteResponse struct {
	MessageHeader
	Tag []byte
}

func (c *Conn) ReadCommandComplete(ctx context.Context, h MessageHeader) (CommandCompleteResponse, error) {
	var zero CommandCompleteResponse
	if err := checkMessageHeader(h, 'C'); err != nil {
		return zero, err
	}
	t, err := c.c.ReadString()
	if err != nil {
		return zero, err
	}
	return CommandCompleteResponse{
		MessageHeader: h,
		Tag:           t,
	}, nil
}

func (c *Conn) WriteDataRow(ctx context.Context, vals ...string) error {
	var s sizer
	s.addInt32() // length
	s.addInt16() // number of columns
	for _, v := range vals {
		s.addBytesString(v)
	}
	c.c.WriteByte('D')
	c.c.WriteInt32(s.size())
	c.c.WriteInt16(int16(len(vals)))
	for _, v := range vals {
		c.c.WriteBytesString(v)
	}
	return c.Err()
}

type DataRowResponse struct {
	MessageHeader
	Values [][]byte
}

func (c *Conn) ReadDataRow(ctx context.Context, h MessageHeader) (DataRowResponse, error) {
	if err := checkMessageHeader(h, 'D'); err != nil {
		return DataRowResponse{}, err
	}
	n, err := c.c.ReadInt16()
	if err != nil {
		return DataRowResponse{}, err
	}
	vals := make([][]byte, n)
	for i := 0; i < int(n); i++ {
		r, err := c.c.ReadBytes()
		if err != nil {
			return DataRowResponse{}, err
		}
		data, err := io.ReadAll(r)
		if err != nil {
			return DataRowResponse{}, err
		}
		vals[i] = data
	}
	return DataRowResponse{
		MessageHeader: h,
		Values:        vals,
	}, nil
}
