// Package febe implements the encoding and decoding of Frontend/Backend
// (FE/BE) messages.
package febe

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"

	"blake.io/pqlite/febe/febeio"
)

// Errors
var (
	// ErrTerminate is returned when the server has terminated a
	// connection.
	ErrTerminated = errors.New("febe: connection terminated by backend")

	// ErrEmptyQuery is returned when the server has terminated a
	// query because it was empty.
	ErrEmptyQuery = errors.New("febe: empty query")
)

// RequestTLS sends the frontend message to request TLS to the server. If
// the backend supports TLS then it returns true without error. If the
// backend does not support TLS then it returns false with no error.
// Otherwise it returns an error.
//
// If ok is true then cn is ready to be upgraded to TLS.
func RequestTLS(cn net.Conn) (ok bool, err error) {
	var b [8]byte

	// length including self
	b[0] = byte(len(b) >> 24)
	b[1] = byte(len(b) >> 16)
	b[2] = byte(len(b) >> 8)
	b[3] = byte(len(b))

	sslRequest := int32(80877103)
	b[4] = byte(sslRequest >> 24)
	b[5] = byte(sslRequest >> 16)
	b[6] = byte(sslRequest >> 8)
	b[7] = byte(sslRequest)

	if _, err := cn.Write(b[:]); err != nil {
		return false, err
	}

	clear(b[:])
	_, err = cn.Read(b[:1])
	if err != nil {
		return false, err
	}
	return b[0] == 'S', nil
}

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

	parameterStatusHook func(Parameter)
}

func New(r io.Reader, w io.Writer) *Conn {
	cn := febeio.NewConn(r, w)
	return &Conn{c: cn}
}

func (c *Conn) SetParameterStatusHook(fn func(Parameter)) {
	c.parameterStatusHook = fn
}

func (c *Conn) checkMessageHeader(want ...byte) error {
	if slices.Contains(want, c.h.Type()) {
		return nil
	}
	return fmt.Errorf("pqlite: febe: unexpected message type: %q want %q", c.h.Type(), string(want))
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

// ReadHeader reads the next febe message header on the Conn after
// discarding any remaining bytes from the previous message. It returns a
// network or deserialization error if any.
//
// As a special case, if the message type is 'E', it will read the error
// response and return an *ErrorFields as the error.
func (c *Conn) ReadHeader(ctx context.Context) (MessageHeader, error) {
	var zero MessageHeader
	t := traceFromContext(ctx)
	for {
		// TODO(bmizerany): We need to thread ctx through to the
		// febeio so we can cancel the read if the user cancels the
		// context. This works for now:
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}

		if err := c.discardRemainingMessageBytes(); err != nil {
			return zero, err
		}
		if err := c.readAndSetCurrentHeader(); err != nil {
			return zero, err
		}
		if t.ReceivedHeader != nil {
			t.ReceivedHeader(ctx, c.h)
		}
		switch c.h.Type() {
		case 'N':
			m, err := c.readNoticeResponse()
			if err != nil {
				return zero, err
			}
			if t.ReceivedNotice != nil {
				t.ReceivedNotice(ctx, m.Notice)
			}
			continue
		case 'E':
			m, err := c.readNoticeResponse()
			if err != nil {
				return zero, err
			}
			e := m.Notice.AsError()
			if t.ReceivedError != nil {
				t.ReceivedError(ctx, e)
			}
			return zero, e
		case 'f': // CopyFail
			e, err := c.readCopyFailResponse()
			if err != nil {
				return zero, err
			}
			if t.ReceivedError != nil {
				t.ReceivedError(ctx, e)
			}
			return zero, e
		case 'S':
			s, err := c.readParameterStatus()
			if err != nil {
				return zero, err
			}
			if c.parameterStatusHook != nil {
				c.parameterStatusHook(Parameter{
					Name:  s.Name,
					Value: s.Value,
				})
			}
			if t.ReceivedParameterStatus != nil {
				t.ReceivedParameterStatus(ctx, Parameter{
					Name:  s.Name,
					Value: s.Value,
				})
			}
			continue
		case 'A':
			m, err := c.ReadNotification()
			if err != nil {
				return zero, err
			}
			if t.ReceivedNotification != nil {
				t.ReceivedNotification(ctx, &m)
			}
			continue
		case 'I':
			return zero, ErrEmptyQuery
		case 'X':
			return zero, ErrTerminated
		default:
			return c.h, c.c.Err()
		}
	}
}

func (c *Conn) readAndSetCurrentHeader() (err error) {
	// TODO(bmizerany): take a context and use it to set a deadline
	var h MessageHeader
	h.msgType, err = c.c.ReadByte()
	if err != nil {
		return err
	}

	// The length includes itself, so reset before reading the length.
	c.c.ResetN()

	h.msgLen, err = c.c.ReadInt32()
	c.h = h
	return err
}

func (c *Conn) discardRemainingMessageBytes() error {
	if c.h.Len() == 0 {
		return nil
	}
	left := c.h.Len() - c.c.N()
	return c.c.Discard(left)
}

// TODO(bmizerany): https://www.postgresql.org/docs/current/errcodes-appendix.html

type sizer int32

func (s *sizer) addByte()            { *s += 1 }
func (s *sizer) addInt32()           { *s += 4 }
func (s *sizer) addInt16()           { *s += 2 }
func (s *sizer) addSizedBytes(n int) { *s += 4 + sizer(n) }
func (s *sizer) addBytes(n int)      { *s += sizer(n) }
func (s *sizer) addString(v string)  { *s += sizer(len(v) + 1) }
func (s *sizer) size() int32         { return int32(*s) }

func (c *Conn) WriteStartupMessage(params ...string) (err error) {
	var s sizer
	s.addInt32() // length
	s.addInt32() // protocol version
	for i := range params {
		k, v, _ := strings.Cut(params[i], "=")
		s.addString(k)
		s.addString(v)
	}
	s.addString("")

	c.c.WriteInt32(s.size())
	c.c.WriteInt32(196608)
	for i := range params {
		k, v, _ := strings.Cut(params[i], "=")
		c.c.WriteString(k)
		c.c.WriteString(v)
	}
	c.c.WriteString("")
	return c.Err()
}

func (c *Conn) WriteTerminate() error {
	c.c.WriteByte('X')
	c.c.WriteInt32(4)
	return c.Err()
}

type AuthenticationType int32

//go:generate go run golang.org/x/tools/cmd/stringer@latest -type=AuthenticationType

const (
	AuthenticationTypeOK            AuthenticationType = 0
	AuthenticationTypeCleartext     AuthenticationType = 3
	AuthenticationTypeMD5           AuthenticationType = 5
	AuthenticationTypeSCMCredential AuthenticationType = 6
	AuthenticationTypeGSS           AuthenticationType = 7
	AuthenticationTypeGSSContinue   AuthenticationType = 8
	AuthenticationTypeSSPI          AuthenticationType = 9
	AuthenticationTypeSASL          AuthenticationType = 10
	AuthenticationTypeSASLContinue  AuthenticationType = 11
	AuthenticationTypeSASLFinal     AuthenticationType = 12
)

type AuthenticationResponse struct {
	MessageHeader
	Type AuthenticationType

	SASLData       []byte
	SASLMechanisms []string

	MD5Salt []byte
}

func (c *Conn) ReadAuthentication() (AuthenticationResponse, error) {
	var zero AuthenticationResponse
	if err := c.checkMessageHeader('R'); err != nil {
		return zero, err
	}

	// Get the type of authentication
	typ, err := c.c.ReadInt32()
	if err != nil {
		return zero, err
	}

	r := AuthenticationResponse{
		MessageHeader: c.h,
		Type:          AuthenticationType(typ),
	}

	switch r.Type {
	case AuthenticationTypeSASL:
		for {
			mech, err := c.c.ReadString()
			if err != nil {
				return zero, err
			}
			if len(mech) == 0 {
				break
			}
			r.SASLMechanisms = append(r.SASLMechanisms, string(mech))
		}
	case AuthenticationTypeSASLContinue, AuthenticationTypeSASLFinal:
		// read the remaining bytes: message body length - size of
		// type (int32)
		rem := c.h.BodyLen() - 4
		data, err := c.c.ReadByteN(rem)
		if err != nil {
			return zero, err
		}
		r.SASLData = data
	case AuthenticationTypeMD5:
		salt, err := c.c.ReadByteN(4)
		if err != nil {
			return zero, err
		}
		r.MD5Salt = salt
	}

	return r, nil
}

func (c *Conn) WritePasswordMessage(password string) error {
	var s sizer
	s.addInt32() // length
	s.addString(password)
	c.c.WriteByte('p')
	c.c.WriteInt32(s.size())
	c.c.WriteString(password)
	return c.Err()
}

func (c *Conn) WriteSASLInitialResponse(mechanism string, data []byte) error {
	var s sizer
	s.addInt32() // length
	s.addString(mechanism)
	s.addSizedBytes(len(data))
	c.c.WriteByte('p')
	c.c.WriteInt32(s.size())
	c.c.WriteString(mechanism)
	c.c.WriteSizedBytes(data)
	return c.Err()
}

func (c *Conn) WriteSASLResponse(data []byte) error {
	var s sizer
	s.addInt32() // length
	s.addBytes(len(data))
	c.c.WriteByte('p')
	c.c.WriteInt32(s.size())
	c.c.Write(data)
	return c.Err()
}

func (c *Conn) ReadReadyForQuery() (ReadyForQueryResponse, error) {
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

func (c *Conn) WriteNotification(pid int32, channel, payload string) error {
	var s sizer
	s.addInt32() // length
	s.addInt32()
	s.addString(channel)
	s.addString(payload)
	c.c.WriteByte('A')
	c.c.WriteInt32(s.size())
	c.c.WriteInt32(pid)
	c.c.WriteString(channel)
	c.c.WriteString(payload)
	return c.Err()
}

func (c *Conn) ReadNotification() (NotificationResponse, error) {
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

func (c *Conn) WriteParse(stmt, sql string) error {
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

type FieldDescription struct {
	Name             string // This is a string to gurarantee that it is a copy, safe for use after a call to any later Read call
	TableOID         int32
	TableAttributeID int16
	DataTypeOID      int32
	DataTypeSize     int16
	TypeModifier     int32
	FormatCode       int16
}

// This is a zero value, so it is safe to use without initialization and
// values of FieldDescription are comparable (e.g. the underlying memory
// cannot be changed (without unsafe) by anymore once it is returned from a
// Read call).
//
//lint:ignore SA4000 We're ensuring that FieldDescription is comparable
var _ = (FieldDescription{}) == (FieldDescription{})

type RowDescriptionResponse struct {
	MessageHeader
	Fields []FieldDescription
}

func (c *Conn) WriteDescribe(typ byte, statment string) error {
	var s sizer
	s.addInt32() // length
	s.addByte()
	s.addString(statment)

	c.c.WriteByte('D')
	c.c.WriteInt32(s.size())
	c.c.WriteByte(typ)
	c.c.WriteString(statment)
	return c.Err()
}

func (c *Conn) ReadRowDescription() (RowDescriptionResponse, error) {
	if err := c.checkMessageHeader('T'); err != nil {
		return RowDescriptionResponse{}, err
	}
	n, err := c.c.ReadInt16()
	if err != nil {
		return RowDescriptionResponse{}, err
	}
	fields := make([]FieldDescription, n)
	for i := range fields {
		name, err := c.c.ReadString()
		if err != nil {
			return RowDescriptionResponse{}, err
		}
		tableOID, err := c.c.ReadInt32()
		if err != nil {
			return RowDescriptionResponse{}, err
		}
		tableAttributeID, err := c.c.ReadInt16()
		if err != nil {
			return RowDescriptionResponse{}, err
		}
		dataTypeOID, err := c.c.ReadInt32()
		if err != nil {
			return RowDescriptionResponse{}, err
		}
		dataTypeSize, err := c.c.ReadInt16()
		if err != nil {
			return RowDescriptionResponse{}, err
		}
		typeModifier, err := c.c.ReadInt32()
		if err != nil {
			return RowDescriptionResponse{}, err
		}
		formatCode, err := c.c.ReadInt16()
		if err != nil {
			return RowDescriptionResponse{}, err
		}
		fields[i] = FieldDescription{
			Name:             string(name),
			TableOID:         tableOID,
			TableAttributeID: tableAttributeID,
			DataTypeOID:      dataTypeOID,
			DataTypeSize:     dataTypeSize,
			TypeModifier:     typeModifier,
			FormatCode:       formatCode,
		}
	}
	return RowDescriptionResponse{
		MessageHeader: c.h,
		Fields:        fields,
	}, nil
}

type ParameterDescriptionResponse struct {
	MessageHeader
	OIDs []int32
}

func (c *Conn) ReadParameterDescription() (ParameterDescriptionResponse, error) {
	if err := c.checkMessageHeader('t'); err != nil {
		return ParameterDescriptionResponse{}, err
	}
	n, err := c.c.ReadInt16()
	if err != nil {
		return ParameterDescriptionResponse{}, err
	}
	oids := make([]int32, n)
	for i := range oids {
		oid, err := c.c.ReadInt32()
		if err != nil {
			return ParameterDescriptionResponse{}, err
		}
		oids[i] = oid
	}
	return ParameterDescriptionResponse{
		MessageHeader: c.h,
		OIDs:          oids,
	}, nil
}

type FormatParams struct {
	In  []int16
	Out []int16
}

var emptyFormatParams = &FormatParams{}

func (c *Conn) WriteBind(portal, statement string, args []string, p *FormatParams) error {
	p = cmp.Or(p, emptyFormatParams)

	var s sizer
	s.addInt32() // length
	s.addString(portal)
	s.addString(statement)

	// parameter format codes
	s.addInt16()
	for range len(p.In) {
		s.addInt16()
	}

	// parameter values
	s.addInt16()
	for _, arg := range args {
		s.addSizedBytes(len(arg))
	}

	// result format codes
	s.addInt16()
	for range len(p.Out) {
		s.addInt16()
	}

	c.c.WriteByte('B')
	c.c.WriteInt32(s.size())
	c.c.WriteString(portal)
	c.c.WriteString(statement)

	c.c.WriteInt16(int16(len(p.In)))
	for i := range len(p.In) {
		c.c.WriteInt16(p.In[i])
	}

	c.c.WriteInt16(int16(len(args)))
	for _, arg := range args {
		if err := c.c.WriteInt32(int32(len(arg))); err != nil {
			return err
		}
		_, err := io.WriteString(c.c, arg)
		if err != nil {
			return err
		}
	}

	c.c.WriteInt16(int16(len(p.Out)))
	for i := range len(p.Out) {
		c.c.WriteInt16(p.Out[i])
	}

	return c.Err()
}

func (c *Conn) WriteSync() error {
	c.c.WriteByte('S')
	c.c.WriteInt32(4)
	return c.Err()
}

func (c *Conn) WriteExecute(portal string, limit int) error {
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

type CommandCompleteResponse struct {
	MessageHeader
	Tag
}

func (c *Conn) ReadCommandComplete() (CommandCompleteResponse, error) {
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

var commands = map[string]string{
	"INSERT": "INSERT",
	"UPDATE": "UPDATE",
	"DELETE": "DELETE",
	"SELECT": "SELECT",
	"MOVE":   "MOVE",
	"FETCH":  "FETCH",
	"COPY":   "COPY",
	"MERGE":  "MERGE",
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

	intern := commands[string(cmd)]
	if intern == "" {
		intern = string(cmd)
	}

	rowsInt, _ := strconv.ParseInt(string(data), 10, 64)
	return Tag{
		Command:      intern,
		RowsAffected: rowsInt,
	}
}

type DataRowResponse struct {
	MessageHeader
	NumCols int16
}

type DataRowValueResponse struct {
	MessageHeader
	Data []byte
}

func (c *Conn) ReadDataRow(discardBinaryHeader bool) (DataRowResponse, error) {
	if err := c.checkMessageHeader('D', 'd'); err != nil {
		return DataRowResponse{}, err
	}
	if discardBinaryHeader {
		if err := c.discardBinarySignatureFlagsAndExt(); err != nil {
			return DataRowResponse{}, err
		}
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

func (c *Conn) ReadDataRowValue() (DataRowValueResponse, error) {
	if err := c.checkMessageHeader('D', 'd'); err != nil {
		return DataRowValueResponse{}, err
	}
	data, err := c.c.ReadBytes()
	if err != nil {
		return DataRowValueResponse{}, err
	}
	return DataRowValueResponse{Data: data}, nil
}
