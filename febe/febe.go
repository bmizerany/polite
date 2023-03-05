package febe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"blake.io/pqlite/febe/febeio"
	"go4.org/mem"
)

type Conn struct {
	mu sync.Mutex
	c  *febeio.Conn
}

func New(r io.Reader, w io.Writer) *Conn {
	c := febeio.NewConn(r, w)
	return &Conn{c: c}
}

func (c *Conn) Flush() error {
	return c.c.Flush()
}

func (c *Conn) Err() error {
	return c.c.Err()
}

func (c *Conn) WriteStartupMessage(ctx context.Context, params ...string) (err error) {
	if len(params)%2 != 0 {
		return errors.New("params must be even")
	}

	var s sizer
	s.addInt32() // length
	s.addInt32() // protocol version
	s.addStrings(params...)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.c.WriteInt32(s.size())
	c.c.WriteInt32(196608)
	for i := 0; i < len(params); i += 2 {
		c.c.WriteString(mem.S(params[i]))
		c.c.WriteString(mem.S(params[i+1]))
	}
	return c.Err()
}

type Message interface {
	Header() *MessageHeader
}

type MessageHeader struct {
	Type byte
	Len  int
}

func (h *MessageHeader) Header() *MessageHeader { return h }

func (c *Conn) Next() (Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := c.c.ReadByte()
	if err != nil {
		return nil, err
	}
	l, err := c.c.ReadInt32()
	if err != nil {
		return nil, err
	}
	h := MessageHeader{Type: b, Len: int(l)}
	switch b {
	case 'E':
		return c.readErrorResponse(h)
	default:
		return nil, fmt.Errorf("pqlite: unexpected message type %q", b)
	}
}

// TODO(bmizerany): https://www.postgresql.org/docs/current/errcodes-appendix.html

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

type sizer int32

func (s *sizer) addInt32()          { *s += 4 }
func (s *sizer) addInt16()          { *s += 2 }
func (s *sizer) addByte()           { *s += 1 }
func (s *sizer) addBytes(v []byte)  { *s += sizer(len(v)) }
func (s *sizer) addString(v string) { *s += sizer(len(v) + 1) }
func (s *sizer) addStrings(vv ...string) {
	for _, v := range vv {
		s.addString(v)
	}
}
func (s *sizer) size() int32 { return int32(*s) }
