package febe

import (
	"context"
	"fmt"
	"strings"
)

type Async struct {
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

func (e *Async) Fields(yield func(fieldID byte, val string) bool) {
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

type Notice Async

// Error represents an error received from the backend.
type Error Async

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("postgres: ")
	b.WriteString(e.Message)
	return b.String()
}

type AsyncMessage struct {
	MessageHeader
	*Async
}

func (c *Conn) ReadNotice(ctx context.Context) (*Notice, error) {
	if err := c.checkMessageHeader('N'); err != nil {
		return nil, err
	}
	m, err := c.readErrorResponse(ctx)
	if err != nil {
		return nil, err
	}
	return (*Notice)(m.Async), nil
}

func (c *Conn) ReadError(ctx context.Context) (*Error, error) {
	if err := c.checkMessageHeader('E'); err != nil {
		return nil, err
	}
	m, err := c.readErrorResponse(ctx)
	if err != nil {
		return nil, err
	}
	return (*Error)(m.Async), nil
}

func (c *Conn) readErrorResponse(ctx context.Context) (AsyncMessage, error) {
	var zero AsyncMessage
	e := AsyncMessage{
		MessageHeader: c.h,
		Async:         &Async{},
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
			e.Severity = string(v)
		case 'C':
			e.Code = string(v)
		case 'M':
			e.Message = string(v)
		case 'D':
			e.Detail = string(v)
		case 'H':
			e.Hint = string(v)
		case 'P':
			e.Position = string(v)
		case 'p':
			e.Internal = string(v)
		case 'W':
			e.Where = string(v)
		case 's':
			e.Schema = string(v)
		case 't':
			e.Table = string(v)
		case 'c':
			e.Column = string(v)
		case 'd':
			e.DataType = string(v)
		case 'n':
			e.Constraint = string(v)
		case 'F':
			e.File = string(v)
		case 'L':
			e.Line = string(v)
		case 'R':
			e.Routine = string(v)
		case 'V':
			e.Verbosity = string(v)
		default:
			return zero, fmt.Errorf("pqlite: unexpected error response key %q", k)
		}
	}
	return e, nil
}
