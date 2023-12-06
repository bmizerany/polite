package febe

import (
	"context"
	"fmt"
	"strings"
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

func (n *Notice) AsError() *Error {
	return &Error{Notice: n}
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("postgres: ")
	b.WriteString(e.Message)
	return b.String()
}

type NoticeResponse struct {
	MessageHeader
	*Notice
}

func (c *Conn) readNoticeResponse(ctx context.Context) (NoticeResponse, error) {
	if err := c.checkMessageHeader('N', 'E'); err != nil {
		return NoticeResponse{}, err
	}
	var zero NoticeResponse
	n := NoticeResponse{
		MessageHeader: c.h,
		Notice:        &Notice{},
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
			n.Severity = string(v)
		case 'C':
			n.Code = string(v)
		case 'M':
			n.Message = string(v)
		case 'D':
			n.Detail = string(v)
		case 'H':
			n.Hint = string(v)
		case 'P':
			n.Position = string(v)
		case 'p':
			n.Internal = string(v)
		case 'W':
			n.Where = string(v)
		case 's':
			n.Schema = string(v)
		case 't':
			n.Table = string(v)
		case 'c':
			n.Column = string(v)
		case 'd':
			n.DataType = string(v)
		case 'n':
			n.Constraint = string(v)
		case 'F':
			n.File = string(v)
		case 'L':
			n.Line = string(v)
		case 'R':
			n.Routine = string(v)
		case 'V':
			n.Verbosity = string(v)
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

func (c *Conn) ReadParameterStatus(ctx context.Context) (ParameterStatusResponse, error) {
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
