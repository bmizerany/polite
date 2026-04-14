package febe

import (
	"bytes"
	"context"
	"testing"
)

func FuzzReadHeader(f *testing.F) {
	// seed uses a Conn to write well-formed messages into a buffer
	// and returns the raw bytes.
	seed := func(fn func(c *Conn)) []byte {
		var buf bytes.Buffer
		c := New(&buf, &buf)
		fn(c)
		c.Flush()
		return buf.Bytes()
	}

	// Fatal error — consumed by ReadHeader, returned as *Error
	f.Add(seed(func(c *Conn) {
		c.WriteNotice('E', &Notice{
			Severity:  "FATAL",
			Verbosity: "FATAL",
			Code:      "08006",
			Message:   "connection lost",
		})
	}))

	// Non-fatal error — returned as *Error but doesn't poison conn
	f.Add(seed(func(c *Conn) {
		c.WriteNotice('E', &Notice{
			Severity:  "ERROR",
			Verbosity: "ERROR",
			Code:      "42601",
			Message:   "syntax error",
		})
	}))

	// Notice — consumed silently; terminate after so ReadHeader returns
	f.Add(seed(func(c *Conn) {
		c.WriteNotice('N', &Notice{Severity: "WARNING", Message: "advisory"})
		c.WriteTerminate()
	}))

	// Notification — consumed silently
	f.Add(seed(func(c *Conn) {
		c.WriteNotification(1234, "channel", "payload")
		c.WriteTerminate()
	}))

	// ParameterStatus — consumed silently (no Write method; encode by hand)
	f.Add(rawMsg('S', cstring("client_encoding"), cstring("UTF8")))

	// CopyFail
	f.Add(rawMsg('f', cstring("copy failed")))

	// EmptyQuery / Terminate / pass-through
	f.Add(rawMsg('I'))
	f.Add(seed(func(c *Conn) { c.WriteTerminate() }))
	f.Add(rawMsg('D', []byte("hello")))
	f.Add(rawMsg('Z', []byte{byte(ReadyStatusIdle)}))
	f.Add(rawMsg('T'))

	// Edge cases: empty, truncated, bogus length
	f.Add([]byte{})
	f.Add([]byte{'E'})
	f.Add([]byte{'E', 0, 0})
	f.Add(seed(func(c *Conn) {
		c.WriteNotice('E', &Notice{})
	}))

	f.Fuzz(func(t *testing.T, data []byte) {
		cn := New(bytes.NewReader(data), &bytes.Buffer{})
		ctx := context.Background()
		for range 4 {
			if _, err := cn.ReadHeader(ctx); err != nil {
				return
			}
		}
	})
}

// cstring returns s as a null-terminated byte slice.
func cstring(s string) []byte {
	return append([]byte(s), 0)
}

// rawMsg builds a single wire-protocol message from a type byte and
// body parts, for message types that lack a dedicated Conn.Write method.
func rawMsg(typ byte, parts ...[]byte) []byte {
	var n int
	for _, p := range parts {
		n += len(p)
	}
	n32 := int32(n + 4)
	var buf bytes.Buffer
	buf.WriteByte(typ)
	buf.Write([]byte{byte(n32 >> 24), byte(n32 >> 16), byte(n32 >> 8), byte(n32)})
	for _, p := range parts {
		buf.Write(p)
	}
	return buf.Bytes()
}
