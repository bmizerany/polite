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

	"blake.io/polite/febe/febeio"
	"blake.io/polite/oids"
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

// ReadyStatus reports the backend transaction status in ReadyForQuery.
type ReadyStatus byte

const (
	// ReadyStatusIdle indicates the backend is not in a transaction block.
	ReadyStatusIdle ReadyStatus = 'I'

	// ReadyStatusBusy indicates the backend is in a transaction block.
	ReadyStatusBusy ReadyStatus = 'T'

	// ReadyStatusFailed indicates the backend is in a failed transaction block.
	ReadyStatusFailed ReadyStatus = 'E'
)

var inTransaction = [255]bool{
	ReadyStatusIdle:   false,
	ReadyStatusBusy:   true,
	ReadyStatusFailed: true,
}

// InTransaction reports whether s indicates the backend is inside a
// transaction block, including a failed transaction block.
func (s ReadyStatus) InTransaction() bool {
	return inTransaction[s]
}

var statusNames = [255]string{
	ReadyStatusIdle:   "ReadyStatusIdle",
	ReadyStatusBusy:   "ReadyStatusBusy",
	ReadyStatusFailed: "ReadyStatusFailed",
}

// String returns a debug representation of s.
func (s ReadyStatus) String() string {
	if name := statusNames[s]; name != "" {
		return name
	}
	return fmt.Sprintf("ReadyStatus(<unknown %q>)", byte(s))
}

// ReadyForQueryResponse is the backend ReadyForQuery message.
type ReadyForQueryResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// Status reports the backend transaction status.
	Status ReadyStatus
}

// Conn reads and writes frontend/backend protocol messages.
type Conn struct {
	c *febeio.Conn
	h MessageHeader // the last header read

	readyStatus ReadyStatus

	parameterStatusHook func(Parameter)

	terminatePending bool
	err              error
}

// New returns a Conn that reads from r and writes to w.
func New(r io.Reader, w io.Writer) *Conn {
	cn := febeio.NewConn(r, w)
	return &Conn{c: cn}
}

// SetParameterStatusHook sets fn to receive ParameterStatus messages consumed
// by ReadHeader. A nil fn disables the hook.
func (c *Conn) SetParameterStatusHook(fn func(Parameter)) {
	c.parameterStatusHook = fn
}

func isTimeout(err error) bool {
	type timeout interface {
		Timeout() bool
	}
	t, ok := err.(timeout)
	return ok && t.Timeout()
}

func shouldTraceObservedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrEmptyQuery) || errors.Is(err, ErrTerminated) {
		return false
	}
	var fe *Error
	return !errors.As(err, &fe)
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
	var fe *Error
	switch {
	case errors.Is(err, ErrTerminated):
		c.err = ErrTerminated
	case errors.As(err, &fe):
		if fe.IsFatal() {
			c.err = err
		} else {
			return err
		}
	default:
		c.err = err
	}
	if c.err != nil {
		c.readyStatus = 0
	}
	return c.err
}

func (c *Conn) handleErr(ctx context.Context, err error) error {
	if shouldTraceObservedError(err) {
		TraceObservedError(ctx, err)
	}
	return c.setErr(err)
}

func (c *Conn) checkMessageHeader(want ...byte) error {
	if slices.Contains(want, c.h.Type()) {
		return nil
	}
	return fmt.Errorf("polite: febe: unexpected message type: %q want %q", c.h.Type(), string(want))
}

// Err returns the connection's pending error state.
func (c *Conn) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.setErr(c.c.Err())
}

// Status reports the backend status from the most recent ReadyForQuery
// message.
func (c *Conn) Status() ReadyStatus {
	return c.readyStatus
}

// Flush flushes any buffered writes.
func (c *Conn) Flush() error {
	if c.terminatePending {
		c.terminatePending = false
		return c.c.Flush()
	}
	if c.err != nil {
		return c.err
	}
	return c.setErr(c.c.Flush())
}

// Message is implemented by decoded backend messages that expose their header.
type Message interface {
	Header() MessageHeader
}

// MessageHeader is the decoded header for a single protocol message.
// Its length uses the PostgreSQL wire format: it includes the 4-byte
// length field and excludes the leading message type byte.
type MessageHeader struct {
	msgType byte
	msgLen  int32
}

// Type returns the protocol message type byte.
func (h MessageHeader) Type() byte { return h.msgType }

// Len returns the protocol message length from the header.
// It includes the 4-byte length field and excludes the type byte.
func (h MessageHeader) Len() int { return int(h.msgLen) }

// BodyLen returns the unread message body length.
// It excludes the 4-byte length field.
func (h MessageHeader) BodyLen() int { return int(h.msgLen - 4) }

// Header returns h so MessageHeader implements Message.
func (h MessageHeader) Header() MessageHeader { return h }

// String returns a debug representation of h.
func (h MessageHeader) String() string {
	return fmt.Sprintf("<MessageHeader Type:%q Len:%d>", h.msgType, h.msgLen)
}

// ReadHeader reads the next febe message header on the Conn after
// discarding any remaining bytes from the previous message. It returns a
// network or deserialization error if any.
//
// As a special case, if the message type is 'E', it will read the error
// response and return an *Error.
func (c *Conn) ReadHeader(ctx context.Context) (MessageHeader, error) {
	var zero MessageHeader
	t := traceFromContext(ctx)
	for {
		if c.err != nil {
			return zero, c.err
		}
		if err := c.c.Err(); err != nil && !isTimeout(err) {
			return zero, c.setErr(err)
		}

		// TODO(bmizerany): We need to thread ctx through to the
		// febeio so we can cancel the read if the user cancels the
		// context. This works for now:
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}

		if err := c.discardRemainingMessageBytes(); err != nil {
			return zero, c.handleErr(ctx, err)
		}
		if err := c.readAndSetCurrentHeader(); err != nil {
			return zero, c.handleErr(ctx, err)
		}
		if t.ReceivedHeader != nil {
			t.ReceivedHeader(ctx, c.h)
		}
		switch c.h.Type() {
		case 'N':
			m, err := c.readNoticeResponse()
			if err != nil {
				return zero, c.handleErr(ctx, err)
			}
			if t.ReceivedNotice != nil {
				t.ReceivedNotice(ctx, m.Notice)
			}
			continue
		case 'E':
			m, err := c.readNoticeResponse()
			if err != nil {
				return zero, c.handleErr(ctx, err)
			}
			e := m.AsError()
			if t.ReceivedError != nil {
				t.ReceivedError(ctx, e)
			}
			if e.IsFatal() {
				return zero, c.setErr(e)
			}
			return zero, e
		case 'f': // CopyFail
			e, err := c.readCopyFailResponse()
			if err != nil {
				return zero, c.handleErr(ctx, err)
			}
			if t.ReceivedError != nil {
				t.ReceivedError(ctx, e)
			}
			return zero, c.setErr(e)
		case 'S':
			s, err := c.readParameterStatus()
			if err != nil {
				return zero, c.handleErr(ctx, err)
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
			m, err := c.readNotification()
			if err != nil {
				return zero, c.handleErr(ctx, err)
			}
			if t.ReceivedNotification != nil {
				t.ReceivedNotification(ctx, &m)
			}
			continue
		case 'I':
			return zero, ErrEmptyQuery
		case 'X':
			return zero, c.setErr(ErrTerminated)
		default:
			return c.h, c.Err()
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

// WriteStartupMessage writes a protocol 3.0 startup message.
// params are encoded as "key=value" strings.
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

// WriteTerminate writes a frontend Terminate message.
func (c *Conn) WriteTerminate() error {
	if c.err != nil {
		return c.err
	}
	if err := c.c.Err(); err != nil && !isTimeout(err) {
		return c.setErr(err)
	}
	c.c.WriteByte('X')
	c.c.WriteInt32(4)
	c.terminatePending = true
	c.err = ErrTerminated
	c.readyStatus = 0
	return nil
}

// AuthenticationType identifies the authentication request in an
// AuthenticationResponse.
type AuthenticationType int32

//go:generate go run golang.org/x/tools/cmd/stringer@latest -type=AuthenticationType

const (
	// AuthenticationTypeOK indicates that authentication succeeded.
	AuthenticationTypeOK AuthenticationType = 0

	// AuthenticationTypeCleartext requests a cleartext password.
	AuthenticationTypeCleartext AuthenticationType = 3

	// AuthenticationTypeSCMCredential requests SCM credentials.
	AuthenticationTypeSCMCredential AuthenticationType = 6

	// AuthenticationTypeGSS requests GSSAPI authentication.
	AuthenticationTypeGSS AuthenticationType = 7

	// AuthenticationTypeGSSContinue carries additional GSSAPI data.
	AuthenticationTypeGSSContinue AuthenticationType = 8

	// AuthenticationTypeSSPI requests SSPI authentication.
	AuthenticationTypeSSPI AuthenticationType = 9

	// AuthenticationTypeSASL advertises the supported SASL mechanisms.
	AuthenticationTypeSASL AuthenticationType = 10

	// AuthenticationTypeSASLContinue carries a SASL challenge.
	AuthenticationTypeSASLContinue AuthenticationType = 11

	// AuthenticationTypeSASLFinal carries final SASL data.
	AuthenticationTypeSASLFinal AuthenticationType = 12
)

// AuthenticationResponse is the backend Authentication message.
type AuthenticationResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// Type identifies the authentication request.
	Type AuthenticationType

	// SASLData holds challenge or final data for SASL continuation messages.
	SASLData []byte

	// SASLMechanisms lists the advertised SASL mechanisms.
	SASLMechanisms []string

}

// ReadAuthentication reads an Authentication response.
// It populates the type-specific fields that apply to the decoded message.
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
	}

	return r, nil
}

// WritePasswordMessage writes a frontend PasswordMessage.
func (c *Conn) WritePasswordMessage(password string) error {
	var s sizer
	s.addInt32() // length
	s.addString(password)
	c.c.WriteByte('p')
	c.c.WriteInt32(s.size())
	c.c.WriteString(password)
	return c.Err()
}

// WriteSASLInitialResponse writes a frontend SASLInitialResponse message.
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

// WriteSASLResponse writes a frontend SASLResponse message.
func (c *Conn) WriteSASLResponse(data []byte) error {
	var s sizer
	s.addInt32() // length
	s.addBytes(len(data))
	c.c.WriteByte('p')
	c.c.WriteInt32(s.size())
	c.c.Write(data)
	return c.Err()
}

// ReadReadyForQuery reads a ReadyForQuery response.
func (c *Conn) ReadReadyForQuery() (ReadyForQueryResponse, error) {
	if err := c.checkMessageHeader('Z'); err != nil {
		return ReadyForQueryResponse{}, err
	}
	b, err := c.c.ReadByte()
	if err != nil {
		return ReadyForQueryResponse{}, err
	}
	status := ReadyStatus(b)
	c.readyStatus = status
	return ReadyForQueryResponse{
		MessageHeader: c.h,
		Status:        status,
	}, nil
}

// BackendKeyDataResponse is the backend BackendKeyData message.
type BackendKeyDataResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// PID is the backend process ID.
	PID int32

	// SecretKey is the backend secret key used for cancellation.
	SecretKey int32
}

// ReadBackendKeyData reads a BackendKeyData response.
func (c *Conn) ReadBackendKeyData() (BackendKeyDataResponse, error) {
	if err := c.checkMessageHeader('K'); err != nil {
		return BackendKeyDataResponse{}, err
	}
	pid, err := c.c.ReadInt32()
	if err != nil {
		return BackendKeyDataResponse{}, err
	}
	secretKey, err := c.c.ReadInt32()
	if err != nil {
		return BackendKeyDataResponse{}, err
	}
	return BackendKeyDataResponse{
		MessageHeader: c.h,
		PID:           pid,
		SecretKey:     secretKey,
	}, nil
}

// Notification is the backend NotificationResponse message.
type Notification struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// PID is the backend process ID that sent the notification.
	PID int32

	// Channel is the notification channel name.
	Channel []byte

	// Payload is the notification payload.
	Payload []byte
}

// WriteNotification writes a NotificationResponse message.
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

// WriteNotice writes a backend NoticeResponse ('N') or ErrorResponse ('E')
// message containing the fields from n. The msgType argument selects the
// message type byte.
func (c *Conn) WriteNotice(msgType byte, n *Notice) error {
	var s sizer
	s.addInt32() // length
	for _, v := range n.Fields {
		s.addByte()
		s.addString(v)
	}
	s.addByte() // terminating zero

	c.c.WriteByte(msgType)
	c.c.WriteInt32(s.size())
	for k, v := range n.Fields {
		c.c.WriteByte(k)
		c.c.WriteString(v)
	}
	c.c.WriteByte(0)
	return c.Err()
}

func (c *Conn) readNotification() (Notification, error) {
	if err := c.checkMessageHeader('A'); err != nil {
		return Notification{}, err
	}
	pid, err := c.c.ReadInt32()
	if err != nil {
		return Notification{}, err
	}
	channel, err := c.c.ReadString()
	if err != nil {
		return Notification{}, err
	}
	payload, err := c.c.ReadString()
	if err != nil {
		return Notification{}, err
	}
	return Notification{
		MessageHeader: c.h,
		PID:           pid,
		Channel:       channel,
		Payload:       payload,
	}, nil
}

// WriteParse writes a frontend Parse message.
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

// FieldDescription describes one column in a RowDescriptionResponse.
type FieldDescription struct {
	// Name is copied out of the message so it remains valid after later reads.
	Name string

	// TableOID is the source table OID, or 0 if the column is not a simple
	// reference to a table column.
	TableOID int32

	// TableAttributeID is the source column attribute number, or 0 if the
	// column is not a simple table reference.
	TableAttributeID int16

	// DataTypeOID is the column data type OID.
	DataTypeOID oids.ID

	// DataTypeSize is the column type size in bytes, or -1 for variable-width
	// types.
	DataTypeSize int16

	// TypeModifier is the type modifier, or -1 if none was supplied.
	TypeModifier int32

	// FormatCode is the column format code: 0 for text or 1 for binary.
	FormatCode int16
}

// This is a zero value, so it is safe to use without initialization and
// values of FieldDescription are comparable (e.g. the underlying memory
// cannot be changed (without unsafe) by anyone once it is returned from a
// Read call).
//
//lint:ignore SA4000 We're ensuring that FieldDescription is comparable
var _ = (FieldDescription{}) == (FieldDescription{})

// RowDescriptionResponse is the backend RowDescription message.
type RowDescriptionResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// Fields describes the returned columns in order.
	Fields []FieldDescription
}

// WriteDescribe writes a frontend Describe message for the named statement
// or portal identified by typ.
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

// ReadRowDescription reads a RowDescription response.
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
			DataTypeOID:      oids.ID(dataTypeOID),
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

// ParameterDescriptionResponse is the backend ParameterDescription message.
type ParameterDescriptionResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// OIDs lists the parameter type OIDs in declaration order.
	OIDs []oids.ID
}

// ReadParameterDescription reads a ParameterDescription response.
func (c *Conn) ReadParameterDescription() (ParameterDescriptionResponse, error) {
	if err := c.checkMessageHeader('t'); err != nil {
		return ParameterDescriptionResponse{}, err
	}
	n, err := c.c.ReadInt16()
	if err != nil {
		return ParameterDescriptionResponse{}, err
	}
	ids := make([]oids.ID, n)
	for i := range ids {
		oid, err := c.c.ReadInt32()
		if err != nil {
			return ParameterDescriptionResponse{}, err
		}
		ids[i] = oids.ID(oid)
	}
	return ParameterDescriptionResponse{
		MessageHeader: c.h,
		OIDs:          ids,
	}, nil
}

// FormatParams holds the parameter and result format codes used by WriteBind.
type FormatParams struct {
	// In holds the parameter format codes.
	In []int16

	// Out holds the result-column format codes.
	Out []int16
}

var emptyFormatParams = &FormatParams{}

// WriteBind writes a frontend Bind message.
// Nil args are encoded as NULL parameter values. If p is nil, all parameter
// and result formats default to text.
func (c *Conn) WriteBind(portal, statement string, args [][]byte, p *FormatParams) error {
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
		s.addInt32()
		if arg != nil {
			s.addBytes(len(arg))
		}
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
		n := int32(-1)
		if arg != nil {
			n = int32(len(arg))
		}
		if err := c.c.WriteInt32(n); err != nil {
			return err
		}
		if arg != nil {
			if _, err := c.c.Write(arg); err != nil {
				return err
			}
		}
	}

	c.c.WriteInt16(int16(len(p.Out)))
	for i := range len(p.Out) {
		c.c.WriteInt16(p.Out[i])
	}

	return c.Err()
}

// WriteSync writes a frontend Sync message.
func (c *Conn) WriteSync() error {
	c.c.WriteByte('S')
	c.c.WriteInt32(4)
	return c.Err()
}

// WriteExecute writes a frontend Execute message.
// limit is the maximum number of rows to return; 0 means no limit.
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

// CommandCompleteResponse is the backend CommandComplete message.
type CommandCompleteResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// Tag is the parsed command tag.
	Tag
}

// ReadCommandComplete reads a CommandComplete response.
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

// Tag is the parsed form of a PostgreSQL command tag.
type Tag struct {
	// Command is the command name, such as "SELECT" or "INSERT".
	Command string

	// RowsAffected is the parsed row count, if the tag carries one.
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

// ParseCommandTag parses a PostgreSQL command tag.
// For INSERT tags, the returned row count ignores the inserted OID field.
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

// DataRowResponse is the header of a DataRow or binary COPY row message.
type DataRowResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// NumCols is the number of column values that follow.
	NumCols int16
}

// DataRowValueResponse holds one column value from a DataRow.
type DataRowValueResponse struct {
	// MessageHeader is not populated by ReadDataRowValue.
	MessageHeader

	// Data is the raw bytes for the column value.
	Data []byte
}

// ReadDataRow reads the column count for a DataRow or binary COPY row.
// If discardBinaryHeader is true, it first discards the binary COPY header.
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

// ReadDataRowValue reads the next column value from the current row.
func (c *Conn) ReadDataRowValue() (DataRowValueResponse, error) {
	if err := c.checkMessageHeader('D', 'd'); err != nil {
		return DataRowValueResponse{}, err
	}
	n, err := c.c.ReadInt32()
	if err != nil {
		return DataRowValueResponse{}, err
	}
	if n == -1 {
		return DataRowValueResponse{}, nil
	}
	if n < 0 {
		return DataRowValueResponse{}, errors.New("invalid message length")
	}
	data, err := c.c.ReadByteN(int(n))
	if err != nil {
		return DataRowValueResponse{}, err
	}
	return DataRowValueResponse{Data: data}, nil
}
