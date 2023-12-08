// Package febe implements the encoding and decoding of Frontend/Backend
// (FE/BE) messages.
package febe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"blake.io/postgres/febe/febeio"
	"blake.io/postgres/iter"
	"go4.org/mem"
)

// Errors
var (
	// ErrTerminate is returned when the server has terminated a
	// connection.
	ErrTerminated = fmt.Errorf("febe: terminated")
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

func (c *Conn) checkMessageHeader(want ...byte) error {
	if slices.Contains(want, c.h.Type()) {
		return nil
	}
	return fmt.Errorf("pqlite: febe: unexpected message type: %q want %q", c.h.Type(), want)
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
	msgLen  int32
}

func (h MessageHeader) Type() byte            { return h.msgType }
func (h MessageHeader) Len() int              { return int(h.msgLen) }
func (h MessageHeader) BodyLen() int          { return int(h.msgLen - 4) }
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
	t := traceFromContext(ctx)
	for {
		if err := c.advanceHeader(); err != nil {
			return zero, err
		}
		if t.ReceivedHeader != nil {
			t.ReceivedHeader(ctx, c.h)
		}
		switch c.h.Type() {
		case 'N':
			m, err := c.readNoticeResponse(ctx)
			if err != nil {
				return zero, err
			}
			if t.ReceivedNotice != nil {
				t.ReceivedNotice(ctx, m.Notice)
			}
			continue
		case 'E':
			m, err := c.readNoticeResponse(ctx)
			if err != nil {
				return zero, err
			}
			e := m.Notice.AsError()
			if t.ReceivedError != nil {
				t.ReceivedError(ctx, e)
			}
			return zero, e
		case 'S':
			s, err := c.ReadParameterStatus(ctx)
			if err != nil {
				return zero, err
			}
			if t.ReceivedParameterStatus != nil {
				t.ReceivedParameterStatus(ctx, Parameter{
					Name:  s.Name,
					Value: s.Value,
				})
			}
			continue
		case 'X':
			return zero, ErrTerminated
		default:
			return c.h, c.c.Err()
		}
	}
}

func (c *Conn) advanceHeader() (err error) {
	// TODO(bmizerany): take a context and use it to set a deadline
	var h MessageHeader
	h.msgType, err = c.c.ReadByte()
	if err != nil {
		return err
	}
	h.msgLen, err = c.c.ReadInt32()
	c.h = h
	return err
}

func (c *Conn) Messages(ctx context.Context) iter.Seq2[MessageHeader, error] {
	return func(yield func(MessageHeader, error) bool) {
		for {
			h, err := c.ReadHeader(ctx)
			if err != nil {
				yield(h, err)
				return
			}
			if !yield(h, nil) {
				return
			}
		}
	}
}

func (c *Conn) DiscardMessage(ctx context.Context) error {
	return c.c.ReadByteN(c.h.BodyLen()).Close()
}

// TODO(bmizerany): https://www.postgresql.org/docs/current/errcodes-appendix.html

type sizer int32

func (s *sizer) addByte()           { *s += 1 }
func (s *sizer) addInt32()          { *s += 4 }
func (s *sizer) addInt16()          { *s += 2 }
func (s *sizer) addBytesN(n int64)  { *s += 4 + sizer(n) }
func (s *sizer) addString(v string) { *s += sizer(len(v) + 1) }
func (s *sizer) size() int32        { return int32(*s) }

func (c *Conn) WriteStartupMessage(ctx context.Context, params ...string) (err error) {
	var s sizer
	s.addInt32() // length
	s.addInt32() // protocol version
	for i := 0; i < len(params); i++ {
		k, v, _ := strings.Cut(params[i], "=")
		s.addString(k)
		s.addString(v)
	}
	s.addString("")

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
	s.addString(query)

	c.c.WriteByte('Q')
	c.c.WriteInt32(s.size())
	c.c.WriteString(query)
	return c.Err()
}

func (c *Conn) WriteParse(ctx context.Context, stmt, sql string) error {
	var s sizer
	s.addInt32() // length
	s.addString(stmt)
	s.addString(sql)
	s.addInt16() // 0 for no param types

	c.c.WriteByte('P')
	c.c.WriteInt32(s.size())
	c.c.WriteString(stmt)
	c.c.WriteString(sql)
	c.c.WriteInt16(0) // 0 for no param types
	return c.Err()

}

func (c *Conn) WriteBind(ctx context.Context, portal, statement string, args [][]byte) error {
	var s sizer
	s.addInt32() // length
	s.addString(portal)
	s.addString(statement)
	s.addInt16() // 0 format codes
	s.addInt16() // number of parameters
	for _, arg := range args {
		s.addBytesN(int64(len(arg)))
	}
	s.addInt16() // 0 result format codes

	c.c.WriteByte('B')
	c.c.WriteInt32(s.size())
	c.c.WriteString(portal)
	c.c.WriteString(statement)
	c.c.WriteInt16(0) // format code
	c.c.WriteInt16(int16(len(args)))
	for _, arg := range args {
		c.c.WriteBytes(arg)
	}
	c.c.WriteInt16(0) // number of result columns
	return c.Err()
}

func (c *Conn) WriteSync(ctx context.Context) error {
	c.c.WriteByte('S')
	c.c.WriteInt32(4)
	return c.Err()
}

func (c *Conn) WriteExecute(ctx context.Context, portal string, limit int) error {
	var s sizer
	s.addInt32() // length
	s.addString(portal)
	s.addInt32() // limit

	c.c.WriteByte('E')
	c.c.WriteInt32(s.size())
	c.c.WriteString(portal)
	c.c.WriteInt32(int32(limit))
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

type CommandCompleteResponse struct {
	MessageHeader
	Tag
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
		Tag:           ParseCommandTag(t),
	}, nil
}

var space = []byte{' '}

type Tag struct {
	Command      string
	RowsAffected int64
}

func ParseCommandTag(tag []byte) Tag {
	cmd, data, _ := bytes.Cut(tag, space)
	if bytes.Equal(cmd, []byte("INSERT")) {
		_, data, _ = bytes.Cut(data, space)
	}
	// TODO(bmizerany): check for UPDATE/DELETE/ETC and
	// skip ParseInt if not of type that is returning
	// rowsAffected. This _could_? return non-zero rows
	// affected when it shouldn't?
	rowsInt, _ := mem.ParseInt(mem.B(data), 10, 64)
	return Tag{
		Command:      string(cmd),
		RowsAffected: rowsInt,
	}
}

type DataRowResponse struct {
	MessageHeader
	NumCols int16
}

type DataRowValueResponse struct {
	MessageHeader
	io.ReadCloser
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
