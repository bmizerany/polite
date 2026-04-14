package febe

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
)

// encodeMsg builds a single PostgreSQL wire-protocol message: type byte,
// 4-byte big-endian length (including itself), then body.
func encodeMsg(typ byte, body []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(typ)
	binary.Write(&buf, binary.BigEndian, int32(len(body)+4))
	buf.Write(body)
	return buf.Bytes()
}

// cstring returns s as a null-terminated byte slice.
func cstring(s string) []byte {
	return append([]byte(s), 0)
}

func FuzzReadHeader(f *testing.F) {
	// Helper to build notice/error bodies: sequence of (key, value\0) pairs
	// terminated by a zero byte.
	noticeBody := func(fields map[byte]string) []byte {
		var b bytes.Buffer
		for k, v := range fields {
			b.WriteByte(k)
			b.Write(cstring(v))
		}
		b.WriteByte(0)
		return b.Bytes()
	}

	// Error (fatal) — consumed by ReadHeader, returned as *Error
	f.Add(encodeMsg('E', noticeBody(map[byte]string{
		'S': "FATAL",
		'V': "FATAL",
		'C': "08006",
		'M': "connection lost",
	})))

	// Error (non-fatal) — returned as *Error but doesn't poison conn
	f.Add(encodeMsg('E', noticeBody(map[byte]string{
		'S': "ERROR",
		'V': "ERROR",
		'C': "42601",
		'M': "syntax error",
	})))

	// Notice — consumed silently, followed by a Terminate so ReadHeader
	// returns instead of blocking.
	notice := encodeMsg('N', noticeBody(map[byte]string{
		'S': "WARNING",
		'M': "advisory",
	}))
	f.Add(append(notice, encodeMsg('X', nil)...))

	// ParameterStatus — consumed silently
	paramBody := append(cstring("client_encoding"), cstring("UTF8")...)
	f.Add(append(encodeMsg('S', paramBody), encodeMsg('X', nil)...))

	// Notification (Async) — consumed silently
	var notifBody bytes.Buffer
	binary.Write(&notifBody, binary.BigEndian, int32(1234)) // PID
	notifBody.Write(cstring("channel"))
	notifBody.Write(cstring("payload"))
	f.Add(append(encodeMsg('A', notifBody.Bytes()), encodeMsg('X', nil)...))

	// CopyFail
	f.Add(encodeMsg('f', cstring("copy failed")))

	// EmptyQuery
	f.Add(encodeMsg('I', nil))

	// Terminate
	f.Add(encodeMsg('X', nil))

	// Pass-through (e.g. DataRow, ReadyForQuery)
	f.Add(encodeMsg('D', []byte("hello")))
	f.Add(encodeMsg('Z', []byte{byte(ReadyStatusIdle)}))

	// Minimal message — just header, no body
	f.Add(encodeMsg('T', nil))

	// Empty input
	f.Add([]byte{})

	// Truncated header
	f.Add([]byte{'E'})
	f.Add([]byte{'E', 0, 0})

	// Bogus length
	f.Add(encodeMsg('E', nil)) // length 4, no body, then EOF on field read

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		cn := New(r, &bytes.Buffer{})
		ctx := context.Background()

		// Call ReadHeader up to a few times to exercise the loop
		// (notices, parameter status, and notifications are consumed
		// internally and loop back to read the next message).
		for range 4 {
			_, err := cn.ReadHeader(ctx)
			if err != nil {
				return
			}
		}
	})
}
