package febe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"blake.io/pqlite/febe/febeio"
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
	var b bytes.Buffer
	c := New(&b, &b)

	want := &ErrorResponse{
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

	c.WriteErrorResponse(context.Background(), want)
	c.Flush()

	want.MessageHeader = MessageHeader{
		typ: 'E',
		len: b.Len() - 1,
	}

	h, err := c.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.ReadErrorResponse(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}

	diff.Test(t, t.Errorf, got, want)
}

func BenchmarkWriteErrorResponse(b *testing.B) {
	c := New(nil, io.Discard)
	b.ReportAllocs()
	e := &ErrorResponse{
		Severity: "ERROR",
		Message:  "MESSAGE",
		Code:     "CODE",
	}
	for i := 0; i < b.N; i++ {
		c.WriteErrorResponse(context.Background(), e)
	}
}

func BenchmarkReadErrorResponse(b *testing.B) {
	b.ReportAllocs()

	const data = "E\x00\x00\x00\x05\x00"

	r := strings.NewReader("")
	c := New(r, nil)
	for i := 0; i < b.N; i++ {
		r.Reset(data)
		h, err := c.ReadHeader()
		if err != nil {
			b.Fatal(err)
		}
		_, err = c.ReadErrorResponse(context.Background(), h)
		if err != nil {
			b.Fatal(err)
		}
	}
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
