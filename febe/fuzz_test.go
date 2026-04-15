package febe

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

func FuzzReadHeader(f *testing.F) {
	// seed writes a well-formed message and adds the raw bytes to
	// the fuzz corpus.
	seed := func(fn func(c *Conn)) {
		var buf bytes.Buffer
		c := New(&buf, &buf)
		fn(c)
		c.Flush()
		f.Add(buf.Bytes())
	}

	// Fatal error — consumed by ReadHeader, returned as *Error
	seed(func(c *Conn) {
		c.WriteNotice('E', &Notice{
			Severity:  "FATAL",
			Verbosity: "FATAL",
			Code:      "08006",
			Message:   "connection lost",
		})
	})

	// Non-fatal error — returned as *Error but doesn't poison conn
	seed(func(c *Conn) {
		c.WriteNotice('E', &Notice{
			Severity:  "ERROR",
			Verbosity: "ERROR",
			Code:      "42601",
			Message:   "syntax error",
		})
	})

	// Notice — consumed silently; terminate after so ReadHeader returns
	seed(func(c *Conn) {
		c.WriteNotice('N', &Notice{Severity: "WARNING", Message: "advisory"})
		c.WriteTerminate()
	})

	// Notification — consumed silently
	seed(func(c *Conn) {
		c.WriteNotification(1234, "channel", "payload")
		c.WriteTerminate()
	})

	// ParameterStatus — consumed silently
	seed(func(c *Conn) {
		c.WriteParameterStatus("client_encoding", "UTF8")
	})

	// CopyFail
	seed(func(c *Conn) {
		c.WriteCopyFail("copy failed")
	})

	// EmptyQuery / Terminate / pass-through
	seed(func(c *Conn) { c.WriteEmptyQuery() })
	seed(func(c *Conn) { c.WriteTerminate() })
	seed(func(c *Conn) { c.WriteReadyForQuery(ReadyStatusIdle) })
	seed(func(c *Conn) {
		c.WriteRowDescription([]FieldDescription{
			{Name: "id", DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1},
		})
	})

	// Edge cases: empty, truncated, bogus length
	f.Add([]byte{})
	f.Add([]byte{'E'})
	f.Add([]byte{'E', 0, 0})
	seed(func(c *Conn) {
		c.WriteNotice('E', &Notice{})
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		cn := New(bytes.NewReader(data), &bytes.Buffer{})
		ctx := context.Background()
		for range 4 {
			_, err := cn.ReadHeader(ctx)
			if err == nil {
				continue
			}

			// The reader is a bytes.Reader, which only
			// produces io.EOF. Every error from ReadHeader
			// must therefore be one of the types below, or a
			// bug.
			switch {
			case errors.As(err, new(*Error)):
			case errors.Is(err, ErrTerminated):
			case errors.Is(err, ErrEmptyQuery):
			case errors.Is(err, errShortMessage):
			case errors.Is(err, io.ErrUnexpectedEOF):
			default:
				t.Fatalf("unexpected error type %T: %v", err, err)
			}
			return
		}
	})
}

