package febe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"blake.io/pqlite/febe/febeio"
	"go4.org/mem"
	"kr.dev/diff"
)

func TestError(t *testing.T) {
	c := New(nil, errorWriter{}) // panics if a Read is called
	for i := 0; i < 3; i++ {
		err := c.WriteStartupMessage(context.Background())
		if i == 0 && err != nil {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		} else if i > 0 && !errors.Is(err, errTest) {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		}
		if !errors.Is(c.Flush(), errTest) {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		}
		if !errors.Is(c.Err(), errTest) {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		}
	}
}

type msgBuffer struct {
	*febeio.Conn
	b bytes.Buffer
}

func newMsgBuffer(t byte) *msgBuffer {
	b := &msgBuffer{}
	b.Conn = febeio.NewConn(nil, &b.b)
	b.WriteByte(t)
	b.WriteInt32(0) // reserved for length
	return b
}

func (b *msgBuffer) Flush() {
	b.Conn.Flush()
	l := b.b.Len() - 1
	b.b.Bytes()[1] = byte(l >> 24)
	b.b.Bytes()[2] = byte(l >> 16)
	b.b.Bytes()[3] = byte(l >> 8)
	b.b.Bytes()[4] = byte(l)
}

func (b *msgBuffer) Reader() io.Reader {
	return &b.b
}

func TestErrorResponse(t *testing.T) {
	w := newMsgBuffer('E')
	w.WriteByte('S')
	w.WriteString(mem.S("ERROR"))
	w.WriteByte('M')
	w.WriteString(mem.S("MESSAGE"))
	w.WriteByte('C')
	w.WriteString(mem.S("CODE"))
	w.WriteByte('D')
	w.WriteString(mem.S("DETAIL"))
	w.WriteByte('H')
	w.WriteString(mem.S("HINT"))
	w.WriteByte('P')
	w.WriteString(mem.S("POS"))
	w.WriteByte('p')
	w.WriteString(mem.S("INTERNAL"))
	w.WriteByte('W')
	w.WriteString(mem.S("WHERE"))
	w.WriteByte('s')
	w.WriteString(mem.S("SCHEMA"))
	w.WriteByte('t')
	w.WriteString(mem.S("TABLE"))
	w.WriteByte('c')
	w.WriteString(mem.S("COLUMN"))
	w.WriteByte('d')
	w.WriteString(mem.S("DATATYPE"))
	w.WriteByte('n')
	w.WriteString(mem.S("CONSTRAINT"))
	w.WriteByte('F')
	w.WriteString(mem.S("FILE"))
	w.WriteByte('L')
	w.WriteString(mem.S("LINE"))
	w.WriteByte('R')
	w.WriteString(mem.S("ROUTINE"))
	w.WriteByte(0)
	w.Flush()

	c := New(w.Reader(), nil)
	got, err := c.Next()
	if err != nil {
		t.Fatal(err)
	}

	want := &ErrorResponse{
		MessageHeader: MessageHeader{
			Type: 'E',
			Len:  129,
		},
		Severity:   "ERROR",
		Message:    "MESSAGE",
		Code:       "CODE",
		Detail:     "DETAIL",
		Hint:       "HINT",
		Position:   "POS",
		Internal:   "INTERNAL",
		Where:      "WHERE",
		Schema:     "SCHEMA",
		Table:      "TABLE",
		Column:     "COLUMN",
		DataType:   "DATATYPE",
		Constraint: "CONSTRAINT",
		File:       "FILE",
		Line:       "LINE",
		Routine:    "ROUTINE",
	}

	diff.Test(t, t.Errorf, got, want)
}

func TestStartupResponse(t *testing.T) {
	var b bytes.Buffer

	ctx := context.Background()
	c := New(&b, &b)
	c.WriteStartupMessage(ctx, "user", "test")
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}
}

var errTest = errors.New("febe_test: test error")

// errorWriter is an io.Writer that always returns an error.
type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("errorWriter.Write: %w", errTest)
}
