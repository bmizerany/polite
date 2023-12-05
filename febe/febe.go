// Package febe implements the encoding and decoding of Frontend/Backend
// (FE/BE) messages.
package febe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"blake.io/postgres/febe/febeio"
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

type Conn struct {
	c *febeio.Conn
	h MessageHeader // the last header read
}

func New(r io.Reader, w io.Writer) *Conn {
	c := febeio.NewConn(r, w)
	return &Conn{c: c}
}

func (c *Conn) checkMessageHeader(want byte) error {
	if c.h.Type() != want {
		return fmt.Errorf("pqlite: febe: unexpected message type: %q want %q", c.h.Type(), want)
	}
	return nil
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

// ReadHeader reads the next febe message header on the Conn. It returns a
// network or deserialization error if any.
//
// As a special case, if the message type is 'E', it will read the error
// response and return an *ErrorFields as the error.
func (c *Conn) ReadHeader(ctx context.Context) (MessageHeader, error) {
	var zero MessageHeader
	b, err := c.c.ReadByte()
	if err != nil {
		return zero, err
	}
	l, err := c.c.ReadInt32()
	if err != nil {
		return zero, err
	}
	c.h = MessageHeader{msgType: b, msgLen: int(l)}
	if c.h.Type() == 'E' {
		er, err := c.ReadError(ctx)
		if err != nil {
			return zero, err
		}
		return zero, er.ErrorFields
	}
	return c.h, nil
}

func (c *Conn) DiscardMessage(ctx context.Context) error {
	return c.c.ReadByteN(c.h.BodyLen()).Close()
}

// TODO(bmizerany): https://www.postgresql.org/docs/current/errcodes-appendix.html

type sizer int32

func (s *sizer) addInt32()               { *s += 4 }
func (s *sizer) addInt16()               { *s += 2 }
func (s *sizer) addByte()                { *s += 1 }
func (s *sizer) addBytesString(v string) { *s += 4 + sizer(len(v)) }
func (s *sizer) addBytesN(n int64)       { *s += 4 + sizer(n) }
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

func (c *Conn) ReadAuthentication(ctx context.Context) (AuthenticationResponse, error) {
	var zero AuthenticationResponse
	if err := c.checkMessageHeader('R'); err != nil {
		return zero, err
	}
	t, err := c.c.ReadInt32()
	if err != nil {
		return zero, err
	}
	return AuthenticationResponse{
		MessageHeader: c.h,
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

func (c *Conn) ReadReadyForQuery(ctx context.Context) (ReadyForQueryResponse, error) {
	if err := c.checkMessageHeader('Z'); err != nil {
		return ReadyForQueryResponse{}, err
	}
	b, err := c.c.ReadByte()
	if err != nil {
		return ReadyForQueryResponse{}, err
	}
	return ReadyForQueryResponse{
		MessageHeader: c.h,
		Status:        ReadyStatus(b),
	}, nil
}

type NotificationResponse struct {
	MessageHeader
	PID     int32
	Channel []byte
	Payload []byte
}

func (c *Conn) ReadNotification(ctx context.Context) (NotificationResponse, error) {
	if err := c.checkMessageHeader('A'); err != nil {
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
		MessageHeader: c.h,
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

func (c *Conn) WriteEmptyQuery(ctx context.Context) error {
	c.c.WriteByte('I')
	c.c.WriteInt32(4)
	return c.Err()
}

type EmptyQueryResponse struct {
	MessageHeader
}

func (c *Conn) ReadEmptyQuery(ctx context.Context) (EmptyQueryResponse, error) {
	if err := c.checkMessageHeader('I'); err != nil {
		return EmptyQueryResponse{}, err
	}
	return EmptyQueryResponse{MessageHeader: c.h}, nil
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
	RowsAffected int64
}

func (c *Conn) ReadCommandComplete(ctx context.Context) (CommandCompleteResponse, error) {
	var zero CommandCompleteResponse
	if err := c.checkMessageHeader('C'); err != nil {
		return zero, err
	}
	t, err := c.c.ReadString()
	if err != nil {
		return zero, err
	}
	return CommandCompleteResponse{
		MessageHeader: c.h,
		RowsAffected:  ParseCommandCompleteTag(t),
	}, nil
}

var space = []byte{' '}

func ParseCommandCompleteTag(tag []byte) (rowsAffected int64) {
	cmd, data, _ := bytes.Cut(tag, space)
	if bytes.Equal(cmd, []byte("INSERT")) {
		_, data, _ = bytes.Cut(data, space)
	}
	// TODO(bmizerany): check for UPDATE/DELETE/ETC and
	// skip ParseInt if not of type that is returning
	// rowsAffected. This _could_? return non-zero rows
	// affected when it shouldn't?
	rowsInt, _ := mem.ParseInt(mem.B(data), 10, 64)
	return rowsInt
}

type DataRowResponse struct {
	MessageHeader
	NumCols int16
}

type DataRowValueResponse struct {
	MessageHeader
	io.ReadCloser
}

func (c *Conn) WriteDataRow(ctx context.Context, cols ...*io.LimitedReader) error {
	var s sizer
	s.addInt32() // length
	s.addInt16() // number of columns
	for _, col := range cols {
		s.addBytesN(col.N)
	}

	c.c.WriteByte('D')
	c.c.WriteInt32(s.size())
	c.c.WriteInt16(int16(len(cols)))
	for _, col := range cols {
		c.c.WriteInt32(int32(col.N))
		_, err := io.Copy(c.c, col) // TODO(bmizerany): use pooled buffer
		if err != nil {
			return err
		}
	}
	return c.Err()
}

func (c *Conn) ReadDataRow(ctx context.Context) (DataRowResponse, error) {
	if err := c.checkMessageHeader('D'); err != nil {
		return DataRowResponse{}, err
	}
	n, err := c.c.ReadInt16()
	if err != nil {
		return DataRowResponse{}, err
	}
	return DataRowResponse{
		MessageHeader: c.h,
		NumCols:       n,
	}, nil
}

func (c *Conn) ReadDataRowValue(ctx context.Context) (DataRowValueResponse, error) {
	if err := c.checkMessageHeader('D'); err != nil {
		return DataRowValueResponse{}, err
	}
	rc, err := c.c.ReadBytes()
	if err != nil {
		return DataRowValueResponse{}, err
	}
	return DataRowValueResponse{ReadCloser: rc}, nil
}

// This comment exits to say; we don't need this method. Most others should
// have parity, but this one is not needed because it is handled best in
// WriteDataRow.
// func (c *Conn) WriteDataRowValue(ctx context.Context, r *io.LimitedReader) error {}
