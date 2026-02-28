package febe

import (
	"fmt"
	"strings"
	"unsafe"
)

type Notice struct {
	Severity   string
	Code       string
	Message    string
	Detail     string
	Hint       string
	Position   string
	Internal   string
	Where      string
	Schema     string
	Table      string
	Column     string
	DataType   string
	Constraint string
	File       string
	Line       string
	Routine    string
	Verbosity  string
}

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
	*Notice
}

// ErrorCode creates an *Error with the given code and empty message.
// This can be conviently used to to check with errors.Is for specific error
// codes without needing to construct a full *Error.
//
// Example:
//
//	if errors.Is(err, febe.ErrorCode("23505")) {
//	    // handle unique violation
//	}
//
// Common errors are defined in the pqlite package for convienience.
func ErrorCode(code string) *Error {
	return &Error{Notice: &Notice{Code: code}}
}

func (e *Notice) AsError() *Error {
	return &Error{Notice: e}
}

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

type NoticeResponse struct {
	MessageHeader
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
			return zero, fmt.Errorf("pqlite: unexpected error response key %q", k)
		}
	}
	return n, nil
}

type ParameterStatusResponse struct {
	MessageHeader
	Name  string
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
