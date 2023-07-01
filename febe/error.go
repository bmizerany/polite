package febe

import (
	"context"
	"fmt"
	"strings"
)

type ErrorType byte

const (
	Error  ErrorType = 'E'
	Notice ErrorType = 'N'
)

type ErrorFields struct {
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

func (e *ErrorFields) Error() string {
	var b strings.Builder
	b.WriteString("postgres: ")
	b.WriteString(e.Message)
	return b.String()
}

func (e *ErrorFields) forEachField(f func(t byte, s string)) {
	f('S', e.Severity)
	f('C', e.Code)
	f('M', e.Message)
	f('D', e.Detail)
	f('H', e.Hint)
	f('P', e.Position)
	f('p', e.Internal)
	f('W', e.Where)
	f('s', e.Schema)
	f('t', e.Table)
	f('c', e.Column)
	f('d', e.DataType)
	f('n', e.Constraint)
	f('F', e.File)
	f('L', e.Line)
	f('R', e.Routine)
}

type ErrorResponse struct {
	MessageHeader
	ErrorFields
}

func (c *Conn) ReadNotice(ctx context.Context, h MessageHeader) (ErrorResponse, error) {
	var zero ErrorResponse
	if err := checkMessageHeader(h, 'N'); err != nil {
		return zero, err
	}
	return c.readErrorResponse(ctx, h)
}

func (c *Conn) ReadError(ctx context.Context, h MessageHeader) (ErrorResponse, error) {
	var zero ErrorResponse
	if err := checkMessageHeader(h, 'E'); err != nil {
		return zero, err
	}
	return c.readErrorResponse(ctx, h)
}

func (c *Conn) readErrorResponse(ctx context.Context, h MessageHeader) (ErrorResponse, error) {
	var zero ErrorResponse
	e := ErrorResponse{MessageHeader: h}
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

func (c *Conn) WriteErrorResponse(ctx context.Context, t ErrorType, e ErrorFields) error {
	var s sizer
	s.addByte()
	s.addInt32()
	e.forEachField(func(t byte, v string) {
		s.addByte()
		s.addString(v)
	})

	c.c.WriteByte(byte(t))
	c.c.WriteInt32(s.size())
	e.forEachField(func(t byte, v string) {
		c.c.WriteByte(t)
		c.c.WriteString(v)
	})
	c.c.WriteByte(0)

	return c.Err()
}
