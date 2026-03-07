package febe

import (
	"fmt"
	"strings"
	"unsafe"
)

// Notice holds the fields shared by PostgreSQL notice and error messages.
// Empty strings mean the backend did not send that field.
type Notice struct {
	// Severity is the localized severity string.
	Severity string

	// Code is the SQLSTATE error code.
	Code string

	// Message is the primary human-readable message.
	Message string

	// Detail is the secondary detail message.
	Detail string

	// Hint is the optional hint text.
	Hint string

	// Position is the reported statement position, as sent by the backend.
	Position string

	// Internal is PostgreSQL protocol field 'p', the internal position.
	Internal string

	// Where is the execution context where the error occurred.
	Where string

	// Schema is the related schema name.
	Schema string

	// Table is the related table name.
	Table string

	// Column is the related column name.
	Column string

	// DataType is the related data type name.
	DataType string

	// Constraint is the related constraint name.
	Constraint string

	// File is the backend source file name.
	File string

	// Line is the backend source line number.
	Line string

	// Routine is the backend source routine name.
	Routine string

	// Verbosity is PostgreSQL protocol field 'V', the non-localized severity.
	Verbosity string
}

// Fields calls yield with each known notice field in protocol key order.
// Fields omitted by the backend are reported as empty strings.
func (e *Notice) Fields(yield func(fieldID byte, val string) bool) {
	fields := []struct {
		k byte
		v string
	}{
		{'S', e.Severity},
		{'C', e.Code},
		{'M', e.Message},
		{'D', e.Detail},
		{'H', e.Hint},
		{'P', e.Position},
		{'p', e.Internal},
		{'W', e.Where},
		{'s', e.Schema},
		{'t', e.Table},
		{'c', e.Column},
		{'d', e.DataType},
		{'n', e.Constraint},
		{'F', e.File},
		{'L', e.Line},
		{'R', e.Routine},
		{'V', e.Verbosity},
	}
	for _, f := range fields {
		if !yield(f.k, f.v) {
			return
		}
	}
}

// Error represents an error received from the backend.
type Error struct {
	// Notice holds the decoded error fields.
	*Notice
}

// ErrorCode creates an *Error with the given code and empty message.
// This can be conveniently used to check with errors.Is for specific error
// codes without needing to construct a full *Error.
//
// Example:
//
//	if errors.Is(err, febe.ErrorCode("23505")) {
//	    // handle unique violation
//	}
//
// Common errors are defined in the polite package for convenience.
func ErrorCode(code string) *Error {
	return &Error{Notice: &Notice{Code: code}}
}

// AsError returns e as an *Error without copying it.
func (e *Notice) AsError() *Error {
	return &Error{Notice: e}
}

// Error returns the backend message prefixed for use as an error string.
func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("postgres: ")
	b.WriteString(e.Message)
	return b.String()
}

// Is reports whether the error is an *Error and with the same Code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// NoticeResponse is a backend NoticeResponse or ErrorResponse message.
type NoticeResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// Notice is the decoded notice body.
	*Notice
}

func (c *Conn) readNoticeResponse() (NoticeResponse, error) {
	if err := c.checkMessageHeader('N', 'E'); err != nil {
		return NoticeResponse{}, err
	}
	var zero NoticeResponse
	n := NoticeResponse{
		MessageHeader: c.h,
		Notice:        &Notice{},
	}

	// Preallocate enough space for the fields in one shot. This
	// prevents an allocation per field. We have to generate some
	// egarbage here, and this is more than we need because it includes
	// the field keys and null terminators, but it's a reasonable
	// approximation and we can avoid allocating for each field.
	buf := make([]byte, 0, c.h.BodyLen())

	// Keep appending to a pre-allocated buffer to avoid a single
	// allocation per field.
	copyToString := func(v []byte) string {
		buf = append(buf[:0], v...)
		s := unsafe.String(unsafe.SliceData(buf), len(buf))
		buf = buf[len(v):]
		return s
	}

	for {
		k, err := c.c.ReadByte()
		if err != nil {
			return zero, err
		}
		if k == 0 {
			break
		}
		v, err := c.c.ReadString()
		if err != nil {
			return zero, err
		}
		switch k {
		case 'S':
			n.Severity = copyToString(v)
		case 'C':
			n.Code = copyToString(v)
		case 'M':
			n.Message = copyToString(v)
		case 'D':
			n.Detail = copyToString(v)
		case 'H':
			n.Hint = copyToString(v)
		case 'P':
			n.Position = copyToString(v)
		case 'p':
			n.Internal = copyToString(v)
		case 'W':
			n.Where = copyToString(v)
		case 's':
			n.Schema = copyToString(v)
		case 't':
			n.Table = copyToString(v)
		case 'c':
			n.Column = copyToString(v)
		case 'd':
			n.DataType = copyToString(v)
		case 'n':
			n.Constraint = copyToString(v)
		case 'F':
			n.File = copyToString(v)
		case 'L':
			n.Line = copyToString(v)
		case 'R':
			n.Routine = copyToString(v)
		case 'V':
			n.Verbosity = copyToString(v)
		default:
			return zero, fmt.Errorf("polite: unexpected error response key %q", k)
		}
	}
	return n, nil
}

// ParameterStatusResponse is the backend ParameterStatus message.
type ParameterStatusResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// Name is the parameter name.
	Name string

	// Value is the parameter value.
	Value string
}

func (c *Conn) readParameterStatus() (ParameterStatusResponse, error) {
	var zero ParameterStatusResponse
	if err := c.checkMessageHeader('S'); err != nil {
		return zero, err
	}
	name, err := c.c.ReadString()
	if err != nil {
		return zero, err
	}
	value, err := c.c.ReadString()
	if err != nil {
		return zero, err
	}
	return ParameterStatusResponse{
		Name:  string(name),
		Value: string(value),
	}, nil
}

func (c *Conn) readCopyFailResponse() (*Error, error) {
	if err := c.checkMessageHeader('f'); err != nil {
		return nil, err
	}
	msg, err := c.c.ReadString()
	if err != nil {
		return nil, err
	}
	return (&Notice{Message: string(msg)}).AsError(), nil
}
