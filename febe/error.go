package febe

import (
	"context"
	"fmt"

	"go4.org/mem"
)

type ErrorResponse struct {
	MessageHeader
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
}

func (e *ErrorResponse) forEachField(f func(t byte, s string)) {
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

func (c *Conn) readErrorResponse(h MessageHeader) (*ErrorResponse, error) {
	e := &ErrorResponse{MessageHeader: h}
	for {
		k, err := c.c.ReadByte()
		if err != nil {
			return nil, err
		}
		if k == 0 {
			break
		}
		v, err := c.c.ReadString()
		if err != nil {
			return nil, err
		}
		switch k {
		case 'S':
			e.Severity = v.StringCopy()
		case 'C':
			e.Code = v.StringCopy()
		case 'M':
			e.Message = v.StringCopy()
		case 'D':
			e.Detail = v.StringCopy()
		case 'H':
			e.Hint = v.StringCopy()
		case 'P':
			e.Position = v.StringCopy()
		case 'p':
			e.Internal = v.StringCopy()
		case 'W':
			e.Where = v.StringCopy()
		case 's':
			e.Schema = v.StringCopy()
		case 't':
			e.Table = v.StringCopy()
		case 'c':
			e.Column = v.StringCopy()
		case 'd':
			e.DataType = v.StringCopy()
		case 'n':
			e.Constraint = v.StringCopy()
		case 'F':
			e.File = v.StringCopy()
		case 'L':
			e.Line = v.StringCopy()
		case 'R':
			e.Routine = v.StringCopy()
		default:
			return nil, fmt.Errorf("pqlite: unexpected error response key %q", k)
		}
	}
	return e, nil
}

func (c *Conn) WriteErrorResponse(ctx context.Context, e *ErrorResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var s sizer
	s.addByte()
	s.addInt32()
	e.forEachField(func(t byte, v string) {
		s.addByte()
		s.addString(v)
	})

	c.c.WriteByte('E')
	c.c.WriteInt32(s.size())
	e.forEachField(func(t byte, v string) {
		c.c.WriteByte(t)
		c.c.WriteString(mem.S(v))
	})
	c.c.WriteByte(0)

	return c.Err()
}
