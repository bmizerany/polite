package febe

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"blake.io/polite/oids"
	"kr.dev/diff"
)

func TestError(t *testing.T) {
	cn := New(nil, errorWriter{}) // panics if a Read is called
	for i := range 3 {
		err := cn.WriteStartupMessage()
		if i == 0 && err != nil {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		} else if i > 0 && !errors.Is(err, errTest) {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		}
		if !errors.Is(cn.Flush(), errTest) {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		}
		if !errors.Is(cn.Err(), errTest) {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		}
	}
}

var testNotice = &Notice{
	Severity:   "ERROR",
	Code:       "CODE",
	Message:    "MESSAGE",
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
	Verbosity:  "VERBOSITY",
}

func TestReadHeaderReadErrorResponse(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.WriteNotice('E', testNotice)
	cn.Flush()
	ctx := context.Background()
	h, err := cn.ReadHeader(ctx)
	diff.Test(t, t.Errorf, h, MessageHeader{})
	diff.Test(t, t.Errorf, err, testNotice.AsError())
}

func TestReadHeaderEmpty(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)

	ctx := context.Background()
	gotH, gotErr := cn.ReadHeader(ctx)
	diff.Test(t, t.Errorf, gotH, MessageHeader{})
	diff.Test(t, t.Errorf, gotErr, io.ErrUnexpectedEOF)
}

func TestReadHeaderDiscard(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	writeMessage(t, &b, 'D', []byte("DISCARD"))
	writeMessage(t, &b, 'Z', nil)
	writeMessage(t, &b, 'X', nil)

	h, err := cn.ReadHeader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h.Type() != 'D' {
		t.Fatalf("h.Type = %c; want D", h.Type())
	}
	if h.Len() != 4+7 {
		t.Fatalf("h.Length = %d; want 4", h.Len())
	}

	h, err = cn.ReadHeader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h.Type() != 'Z' {
		t.Fatalf("h.Type = %c; want Z", h.Type())
	}
	if h.Len() != 4 {
		t.Fatalf("h.Length = %d; want 11", h.Len())
	}

	h, err = cn.ReadHeader(context.Background())
	t.Logf("h = %v; err = %v", h, err)
	if !errors.Is(err, ErrTerminated) {
		t.Fatalf("err = %v; want EOF", err)
	}
}

func TestReadHeaderTracesProtocolError(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.WriteNotice('E', &Notice{
		Severity:  "ERROR",
		Verbosity: "ERROR",
		Code:      "42601",
		Message:   "syntax error",
	})
	cn.Flush()

	var count int
	ctx := WithTrace(context.Background(), &Trace{
		ReceivedError: func(_ context.Context, err error) {
			count++
		},
	})

	_, err := cn.ReadHeader(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := errors.AsType[*Error](err); !ok {
		t.Fatalf("unexpected error type %T: %v", err, err)
	}
	if count != 1 {
		t.Fatalf("ReceivedError called %d times; want 1", count)
	}
}

func TestErrorSeverityNotSticky(t *testing.T) {
	// ERROR severity should not poison the connection.
	var b bytes.Buffer
	cn := New(&b, &b)
	writeReadyForQuery(t, &b, ReadyStatusBusy)
	cn.WriteNotice('E', &Notice{
		Severity:  "ERROR",
		Code:      "42601",
		Message:   "syntax error",
		Verbosity: "ERROR",
	})
	cn.Flush()

	// Write a ReadyForQuery + Terminate after the error so the conn has more to read.
	writeReadyForQuery(t, &b, ReadyStatusIdle)
	writeMessage(t, &b, 'X', nil)

	ctx := context.Background()

	// First call returns the busy status and updates Conn status.
	_, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatalf("ReadHeader() = %v; want nil (got 'Z')", err)
	}
	if _, err := cn.ReadReadyForQuery(); err != nil {
		t.Fatalf("ReadReadyForQuery() = %v; want nil", err)
	}

	// Next call returns the ERROR.
	_, err = cn.ReadHeader(ctx)
	var fe *Error
	if !errors.As(err, &fe) {
		t.Fatalf("ReadHeader() = %v; want *febe.Error", err)
	}
	if fe.IsFatal() {
		t.Fatalf("IsFatal() = true; want false for ERROR severity")
	}
	if got := cn.Status(); got != ReadyStatusBusy {
		t.Fatalf("status after ERROR = %q; want busy", got)
	}

	// Connection should NOT be sticky — next ReadHeader should succeed (returns 'Z').
	h, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatalf("second ReadHeader() = %v; want nil (got 'Z')", err)
	}
	if h.Type() != 'Z' {
		t.Fatalf("h.Type() = %c; want Z", h.Type())
	}
}

func TestTimeoutDoesNotPoisonConnection(t *testing.T) {
	client, server := net.Pipe()

	cn := New(client, io.Discard)

	client.SetReadDeadline(time.Now())
	_, err := cn.ReadHeader(context.Background())
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Errorf("ReadHeader() = %v; want timeout", err)
	}

	go func() { server.Write([]byte{'Z', 0, 0, 0, 5, 'I'}) }()

	client.SetReadDeadline(time.Time{})
	h, err := cn.ReadHeader(context.Background())
	if err != nil {
		t.Fatalf("second ReadHeader() = %v; want nil", err)
	}
	if h.Type() != 'Z' {
		t.Fatalf("h.Type() = %c; want Z", h.Type())
	}
}

func testFatalErrorSticky(t *testing.T, severity string) {
	t.Helper()
	var b bytes.Buffer
	cn := New(&b, &b)
	writeReadyForQuery(t, &b, ReadyStatusBusy)
	cn.WriteNotice('E', &Notice{
		Severity:  severity,
		Code:      "25P03",
		Message:   "terminating connection",
		Verbosity: severity,
	})
	cn.Flush()

	ctx := context.Background()
	_, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatalf("ReadHeader() = %v; want nil (got 'Z')", err)
	}
	if _, err := cn.ReadReadyForQuery(); err != nil {
		t.Fatalf("ReadReadyForQuery() = %v; want nil", err)
	}
	_, err = cn.ReadHeader(ctx)
	var fe *Error
	if !errors.As(err, &fe) {
		t.Fatalf("ReadHeader() = %v; want *febe.Error", err)
	}
	if !fe.IsFatal() {
		t.Fatalf("IsFatal() = false; want true for %s", severity)
	}
	if got := cn.Status(); got != 0 {
		t.Fatalf("status after %s = %q; want zero", severity, got)
	}

	_, err = cn.ReadHeader(ctx)
	if !errors.As(err, &fe) || !fe.IsFatal() {
		t.Fatalf("second ReadHeader() = %v; want sticky fatal *febe.Error", err)
	}
	if err := cn.Flush(); !errors.As(err, &fe) || !fe.IsFatal() {
		t.Fatalf("Flush() = %v; want sticky fatal error", err)
	}
	if err := cn.WriteSync(); !errors.As(err, &fe) || !fe.IsFatal() {
		t.Fatalf("WriteSync() = %v; want sticky fatal error", err)
	}
}

func TestFatalErrorSticky(t *testing.T) { testFatalErrorSticky(t, "FATAL") }

func TestPanicErrorSticky(t *testing.T) { testFatalErrorSticky(t, "PANIC") }

func TestNoticeResponse(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.WriteNotice('N', testNotice)
	cn.Flush()
	writeMessage(t, &b, 'X', nil) // Terminate

	var got *Notice
	ctx := WithTrace(context.Background(), &Trace{
		ReceivedHeader: func(ctx context.Context, h MessageHeader) {
			t.Logf("got header: %v", h)
		},
		ReceivedNotice: func(ctx context.Context, n *Notice) {
			t.Logf("got notice: %v", n)
			if got != nil {
				t.Fatalf("got already set to: %v", got)
			}
			got = n
		},
	})

	_, err := cn.ReadHeader(ctx)
	if !errors.Is(err, ErrTerminated) {
		t.Fatalf("err = %v; want %v", err, ErrTerminated)
	}

	diff.Test(t, t.Errorf, got, testNotice)
}

func TestNotificationResponseTrace(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.WriteNotification(1234, "hello", "world")
	cn.Flush()
	writeMessage(t, &b, 'X', nil)

	var got *Notification
	ctx := WithTrace(context.Background(), &Trace{
		ReceivedNotification: func(ctx context.Context, n *Notification) {
			// Payload is valid only during the callback; copy it
			// now so the assertions survive the next read.
			cp := *n
			cp.payload = n.Append(nil)
			got = &cp
		},
	})

	_, err := cn.ReadHeader(ctx)
	if !errors.Is(err, ErrTerminated) {
		t.Fatalf("err = %v; want %v", err, ErrTerminated)
	}

	want := &Notification{
		MessageHeader: MessageHeader{
			msgType: 'A',
			msgLen:  20,
		},
		PID:     1234,
		Channel: []byte("hello"),
		payload: []byte("world"),
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestWriteStartupMessage(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	if err := cn.WriteStartupMessage("user=test"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Err(); err != nil {
		t.Fatal(err)
	}
	if err := cn.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		// length of message
		0, 0, 0, 19,
		// protocol version
		0, 3, 0, 0,
		// user
		'u', 's', 'e', 'r', 0,
		't', 'e', 's', 't', 0,
		0,
	}
	diff.Test(t, t.Errorf, b.Bytes(), want)
}

func TestWriteTerminate(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	if err := cn.WriteTerminate(); err != nil {
		t.Fatal(err)
	}
	if err := cn.WriteTerminate(); !errors.Is(err, ErrTerminated) {
		t.Fatalf("second WriteTerminate() = %v; want %v", err, ErrTerminated)
	}
	if err := cn.Err(); !errors.Is(err, ErrTerminated) {
		t.Fatalf("Err() = %v; want %v", err, ErrTerminated)
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("buffer len before flush = %d; want 0", got)
	}
	if err := cn.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		// message type
		'X',
		// length of message
		0, 0, 0, 4,
	}
	diff.Test(t, t.Errorf, b.Bytes(), want)
	if err := cn.Flush(); !errors.Is(err, ErrTerminated) {
		t.Fatalf("second Flush() = %v; want %v", err, ErrTerminated)
	}
	diff.Test(t, t.Errorf, b.Bytes(), want)
}

func TestReadReadyForQuery(t *testing.T) {
	tests := []struct {
		name   string
		status ReadyStatus
		isIdle bool
	}{
		{name: "idle", status: ReadyStatusIdle, isIdle: true},
		{name: "busy", status: ReadyStatusBusy, isIdle: false},
		{name: "failed", status: ReadyStatusFailed, isIdle: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b bytes.Buffer
			cn := New(&b, &b)
			writeReadyForQuery(t, &b, tt.status)

			if err := cn.readAndSetCurrentHeader(); err != nil {
				t.Fatal(err)
			}
			got, err := cn.ReadReadyForQuery()
			if err != nil {
				t.Fatal(err)
			}
			want := ReadyForQueryResponse{
				MessageHeader: MessageHeader{
					msgType: 'Z',
					msgLen:  5,
				},
				Status: tt.status,
			}
			diff.Test(t, t.Errorf, got, want)
			if got := cn.Status(); got != tt.status {
				t.Fatalf("Status() = %q; want %q", got, tt.status)
			}
			if got := cn.Status().Idle(); got != tt.isIdle {
				t.Fatalf("Status().Idle() = %v; want %v", got, tt.isIdle)
			}
		})
	}
}

func TestWriteReadyForQuery(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	if err := cn.WriteReadyForQuery(ReadyStatusIdle); err != nil {
		t.Fatal(err)
	}
	if err := cn.Flush(); err != nil {
		t.Fatal(err)
	}

	h, err := cn.ReadHeader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h.Type() != 'Z' {
		t.Fatalf("h.Type() = %c; want Z", h.Type())
	}
	got, err := cn.ReadReadyForQuery()
	if err != nil {
		t.Fatal(err)
	}
	want := ReadyForQueryResponse{
		MessageHeader: MessageHeader{
			msgType: 'Z',
			msgLen:  5,
		},
		Status: ReadyStatusIdle,
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestReadBackendKeyData(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	writeBackendKeyData(t, &b, 1234, 5678)

	if err := cn.readAndSetCurrentHeader(); err != nil {
		t.Fatal(err)
	}

	got, err := cn.ReadBackendKeyData()
	if err != nil {
		t.Fatal(err)
	}

	want := BackendKeyDataResponse{
		MessageHeader: MessageHeader{
			msgType: 'K',
			msgLen:  12,
		},
		PID:       1234,
		SecretKey: 5678,
	}

	diff.Test(t, t.Errorf, got, want)

	_, err = cn.ReadHeader(context.Background())
	diff.Test(t, t.Errorf, err, io.ErrUnexpectedEOF)
}

func TestReadAuthentication(t *testing.T) {
	cases := []struct {
		typ AuthenticationType
	}{
		{typ: AuthenticationTypeOK},
		{typ: AuthenticationTypeCleartext},
	}
	for _, tt := range cases {
		var b bytes.Buffer
		cn := New(&b, &b)

		writeAuthentication(t, &b, tt.typ)

		_, err := cn.ReadHeader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		got, err := cn.ReadAuthentication()
		if err != nil {
			t.Fatal(err)
		}
		want := AuthenticationResponse{
			MessageHeader: MessageHeader{
				msgType: 'R',
				msgLen:  8,
			},
			Type: tt.typ,
		}
		diff.Test(t, t.Errorf, got, want)
	}
}

func TestParseCommandTag(t *testing.T) {
	cases := []struct {
		in   string
		want Tag
	}{
		{in: "SELECT 1", want: Tag{Command: "SELECT", RowsAffected: 1}},
		{in: "INSERT 0 2", want: Tag{Command: "INSERT", RowsAffected: 2}},
		{in: "DELETE 3", want: Tag{Command: "DELETE", RowsAffected: 3}},
		{in: "UPDATE 4", want: Tag{Command: "UPDATE", RowsAffected: 4}},
		{in: "MOVE 5", want: Tag{Command: "MOVE", RowsAffected: 5}},
	}
	for _, tt := range cases {
		t.Run(tt.in, func(t *testing.T) {
			got := ParseCommandTag([]byte(tt.in))
			diff.Test(t, t.Errorf, got, tt.want)
		})

		t.Run(tt.in+" (allocs)", func(t *testing.T) {
			allocs := testing.AllocsPerRun(1000, func() {
				ParseCommandTag([]byte(tt.in))
			})
			if allocs > 0 {
				t.Fatalf("allocs = %v; want 0", allocs)
			}
		})
	}
}

func BenchmarkParseCommandTag(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		ParseCommandTag([]byte("INSERT 0 2"))
	}
}

func TestReadCommandComplete(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	writeMessageCString(t, &b, 'C', "SELECT 1")

	if err := cn.readAndSetCurrentHeader(); err != nil {
		t.Fatal(err)
	}
	got, err := cn.ReadCommandComplete()
	if err != nil {
		t.Fatal(err)
	}
	want := CommandCompleteResponse{
		MessageHeader: MessageHeader{
			msgType: 'C',
			msgLen:  13,
		},
		Tag: Tag{
			Command:      "SELECT",
			RowsAffected: 1,
		},
	}
	diff.Test(t, t.Errorf, got, want)
}

// TODO: CopyInResponse
// The backend is ready to copy data from the frontend to a table; see Section
// 55.2.6.

// TODO: CopyOutResponse
// The backend is ready to copy data from a table to the frontend; see Section
// 55.2.6.

func TestWriteRowDescription(t *testing.T) {
	tests := []struct {
		name   string
		fields []FieldDescription
	}{
		{
			name:   "zero fields",
			fields: []FieldDescription{},
		},
		{
			name: "two fields",
			fields: []FieldDescription{
				{
					Name:             "id",
					TableOID:         100,
					TableAttributeID: 1,
					DataTypeOID:      23, // int4
					DataTypeSize:     4,
					TypeModifier:     -1,
					FormatCode:       123,
				},
				{
					Name:             "name",
					TableOID:         100,
					TableAttributeID: 2,
					DataTypeOID:      25, // text
					DataTypeSize:     -1,
					TypeModifier:     -1,
					FormatCode:       123,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b bytes.Buffer
			cn := New(&b, &b)

			if err := cn.WriteRowDescription(tt.fields); err != nil {
				t.Fatal(err)
			}
			if err := cn.Flush(); err != nil {
				t.Fatal(err)
			}

			h, err := cn.ReadHeader(context.Background())
			if err != nil {
				t.Fatalf("ReadHeader() = %v; want nil", err)
			}
			if h.Type() != 'T' {
				t.Fatalf("h.Type() = %c; want T", h.Type())
			}

			got, err := cn.ReadRowDescription()
			if err != nil {
				t.Fatalf("ReadRowDescription() = %v; want nil", err)
			}

			diff.Test(t, t.Errorf, got.Fields, tt.fields)
		})
	}
}

// DataRow
// One of the set of rows returned by a SELECT, FETCH, etc. query.
func TestReadDataRow(t *testing.T) {
	ctx := context.Background()

	var b bytes.Buffer
	cn := New(&b, &b)
	writeDataRow(t, &b, []byte("hello"), []byte("world"))

	_, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}

	got, err := cn.ReadDataRow(false)
	if err != nil {
		t.Fatal(err)
	}
	want := DataRowResponse{
		MessageHeader: MessageHeader{
			msgType: 'D',
			msgLen:  24,
		},
		NumCols: 2,
	}
	diff.Test(t, t.Errorf, got, want)

	gotValues := make([][]byte, got.NumCols)
	for i := range got.NumCols {
		v, err := cn.ReadDataRowValue()
		if err != nil {
			t.Fatal(err)
		}
		gotValues[i] = bytes.Clone(v.Data)
	}
	wantValues := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}
	diff.Test(t, t.Errorf, gotValues, wantValues)

	_, err = cn.ReadHeader(ctx)
	diff.Test(t, t.Errorf, err, io.ErrUnexpectedEOF)
}

func TestReadEmptyQueryResponse(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	writeMessage(t, &b, 'I', nil)
	if err := cn.Flush(); err != nil {
		t.Fatal(err)
	}
	_, err := cn.ReadHeader(context.Background())
	if !errors.Is(err, ErrEmptyQuery) {
		t.Fatalf("got %v, want %v", err, ErrEmptyQuery)
	}
}

func TestWriteEmptyQuery(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	if err := cn.WriteEmptyQuery(); err != nil {
		t.Fatal(err)
	}
	if err := cn.Flush(); err != nil {
		t.Fatal(err)
	}
	_, err := cn.ReadHeader(context.Background())
	if !errors.Is(err, ErrEmptyQuery) {
		t.Fatalf("got %v, want %v", err, ErrEmptyQuery)
	}
}

func TestReadUnknownTypeChecks(t *testing.T) {
	// Using reflect, iterate over all methods on Conn that start with
	// "Read" and match the generic signature:
	//
	//         func() (T, error)
	//
	// For each one found, create a test that calls the method with the
	// MessageHeader.Type set to '!' and and length of the arbitrary value
	// 1. This should cause an error to be returned.

	var checked int

	to := reflect.TypeFor[*Conn]()

	exempt := map[string]bool{
		"ReadHeader":  true,
		"ReadDataRow": true,
	}

	for i := 0; i < to.NumMethod(); i++ {
		m := to.Method(i)
		if !strings.HasPrefix(m.Name, "Read") {
			continue
		}
		if exempt[m.Name] {
			continue
		}
		t.Run(m.Name, func(t *testing.T) {
			testReadMethodSignature(t, m)

			var b bytes.Buffer
			cn := New(&b, io.Discard)
			writeMessage(t, &b, '!', nil)
			if _, err := cn.ReadHeader(context.Background()); err != nil {
				t.Fatalf("ReadHeader() = %v; want nil", err)
			}
			vm := reflect.ValueOf(cn).MethodByName(m.Name)
			if !vm.IsValid() {
				t.Fatalf("method not found: %s", m.Name)
			}

			// This is only a superficial check that the method is
			// implemented with check that the header type is considered
			// before returning, and returns an error if invalid.
			//
			// This check does not ensure the implementation is checking for the
			// right byte value. This is left up to other tests, mostly
			// integration, which would fail if a ReadX type field check
			// would
			out := vm.Call(nil)
			if err := out[1].Interface().(error); err == nil {
				t.Errorf("expected error, got nil")
			}
			checked++
		})

	}
	if checked == 0 {
		t.Fatal("expected to find at least one Read method to check")
	}
}

func TestReadNotification(t *testing.T) {
	// write a test to check that ReadNotification reads the message
	// from postgres and returns the correct NotificationResponse
	// struct.

	var b bytes.Buffer
	cn := New(&b, &b)
	writeNotification(t, &b, 1234, "hello", "world")

	if err := cn.readAndSetCurrentHeader(); err != nil {
		t.Fatal(err)
	}

	got, err := cn.readNotification()
	if err != nil {
		t.Fatal(err)
	}

	// payload still aliases the read buffer here; it is compared before
	// the ReadHeader below, which would invalidate it.
	want := Notification{
		MessageHeader: MessageHeader{
			msgType: 'A',
			msgLen:  20,
		},
		PID:     1234,
		Channel: []byte("hello"),
		payload: []byte("world"),
	}

	diff.Test(t, t.Errorf, got, want)

	_, err = cn.ReadHeader(context.Background())
	diff.Test(t, t.Errorf, err, io.ErrUnexpectedEOF)
}

// chunkReader streams an endless repetition of msg, returning at most
// chunk bytes per Read, so a large message spans read boundaries the way a
// socket delivers it.
type chunkReader struct {
	msg   []byte
	chunk int
	pos   int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	n := min(len(p), r.chunk)
	for i := range n {
		p[i] = r.msg[r.pos]
		r.pos++
		if r.pos == len(r.msg) {
			r.pos = 0
		}
	}
	return n, nil
}

func TestReadNotificationOwnsFields(t *testing.T) {
	// readNotification must return owned copies, not slices borrowed from
	// the read buffer. With a payload near the 8 KB buffer size, reading
	// the payload compacts the buffer in place over the channel bytes, so
	// a borrowed channel reads back as payload bytes.
	//
	// The first notification leaves the buffer's offset advanced; the
	// second is then read in the steady state where the next read makes
	// room by compacting rather than by reusing the whole buffer. A
	// borrowed channel is corrupted on that second read.
	const channel = "ch"
	payload := strings.Repeat("p", 7000)

	var b bytes.Buffer
	w := New(&b, &b)
	for range 2 {
		if err := w.WriteNotification(1234, channel, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// 4 KB chunks against the 8 KB buffer force the compaction.
	cn := New(&chunkReader{msg: b.Bytes(), chunk: 4096}, io.Discard)
	for i := range 2 {
		if err := cn.readAndSetCurrentHeader(); err != nil {
			t.Fatal(err)
		}
		got, err := cn.readNotification()
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Channel) != channel {
			t.Errorf("notification %d: Channel = %q, want %q", i, got.Channel, channel)
		}
		if p := got.Append(nil); string(p) != payload {
			t.Errorf("notification %d: payload length = %d, want %d", i, len(p), len(payload))
		}
	}
}

func TestWriteParse(t *testing.T) {
	t.Run("no oids", func(t *testing.T) {
		var b bytes.Buffer
		cn := New(&b, &b)

		if err := cn.WriteParse("test", "SELECT 1"); err != nil {
			t.Fatal(err)
		}
		cn.Flush()

		want := []byte{
			'P', 0, 0, 0, 20, // message header
			't', 'e', 's', 't', 0, // portal name
			'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0, // sql
			0, 0, // number of params (int16)
		}
		diff.Test(t, t.Errorf, b.Bytes(), want)
	})

	t.Run("with oids", func(t *testing.T) {
		var b bytes.Buffer
		cn := New(&b, &b)

		if err := cn.WriteParse("s", "SELECT $1, $2", oids.Int4, oids.Text); err != nil {
			t.Fatal(err)
		}
		cn.Flush()

		want := []byte{
			'P', 0, 0, 0, 30, // message header
			's', 0, // statement name
			'S', 'E', 'L', 'E', 'C', 'T', ' ', '$', '1', ',', ' ', '$', '2', 0, // sql
			0, 2, // number of params (int16)
			0, 0, 0, 23, // int4 OID
			0, 0, 0, 25, // text OID
		}
		diff.Test(t, t.Errorf, b.Bytes(), want)
	})
}

func TestWriteBind(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)

	args := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}
	if err := cn.WriteBind("portal", "stmt", args, &FormatParams{
		In:  []int16{1},
		Out: []int16{1, 0},
	}); err != nil {
		t.Fatal(err)
	}
	cn.Flush()

	want := []byte{
		'B',         // message type
		0, 0, 0, 46, // message length
		'p', 'o', 'r', 't', 'a', 'l', 0, // portal name (string)
		's', 't', 'm', 't', 0, // statement name (string)
		0, 1, // number of format codes (int16)
		0, 1, // all binary (int16)
		0, 2, // number of arguments (int16)
		0, 0, 0, 5, // argument length (int32)
		'h', 'e', 'l', 'l', 'o', // argument value
		0, 0, 0, 5, // argument length (int32)
		'w', 'o', 'r', 'l', 'd', // argument value
		0, 2, // number of result format codes
		0, 1, // first binary
		0, 0, // second text
	}
	t.Logf("len:  %d", b.Len())
	t.Logf("got:  %q", b.Bytes())
	t.Logf("want: %q", want)
	diff.Test(t, t.Errorf, b.Bytes(), want)
}

func TestWriteSync(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)

	if err := cn.WriteSync(); err != nil {
		t.Fatal(err)
	}
	cn.Flush()

	want := []byte{
		'S', 0, 0, 0, 4, // message header
	}
	diff.Test(t, t.Errorf, b.Bytes(), want)
}

func TestWriteExecute(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)

	if err := cn.WriteExecute("portal", 0); err != nil {
		t.Fatal(err)
	}
	cn.Flush()

	want := []byte{
		'E',         // message type
		0, 0, 0, 15, // message length
		'p', 'o', 'r', 't', 'a', 'l', 0, // portal name
		0, 0, 0, 0, // max rows
	}
	diff.Test(t, t.Errorf, b.Bytes(), want)
}

func TestReadParameterStatus(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)

	writeParameterStatus(t, &b, "server_version", "9.6.5")
	writeMessage(t, &b, 'X', nil)
	cn.Flush()

	var got []Parameter
	ctx := context.Background()
	ctx = WithTrace(ctx, &Trace{
		ReceivedParameterStatus: func(ctx context.Context, ps Parameter) {
			got = append(got, ps)
		},
	})

	h, err := cn.ReadHeader(ctx)
	t.Logf("header: %v", h)
	if !errors.Is(err, ErrTerminated) {
		t.Fatal(err)
	}

	want := []Parameter{
		{
			Name:  "server_version",
			Value: "9.6.5",
		},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestWriteParameterStatus(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)

	if err := cn.WriteParameterStatus("client_encoding", "UTF8"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := cn.readAndSetCurrentHeader(); err != nil {
		t.Fatal(err)
	}
	got, err := cn.readParameterStatus()
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "client_encoding" {
		t.Errorf("Name = %q; want %q", got.Name, "client_encoding")
	}
	if got.Value != "UTF8" {
		t.Errorf("Value = %q; want %q", got.Value, "UTF8")
	}
}

var errTest = errors.New("febe_test: test error")

// errorWriter is an io.Writer that always returns an error.
type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("errorWriter.Write: %w", errTest)
}

func testReadMethodSignature(t *testing.T, m reflect.Method) {
	t.Helper()
	exempt := map[string]bool{
		"ReadError":  true,
		"ReadNotice": true,
	}
	if exempt[m.Name] {
		return
	}
	if strings.HasSuffix(m.Name, "Response") {
		t.Fatalf("%s should not end with Response", m.Name)
	}
	if m.Type.NumIn() != 1 || m.Type.NumOut() != 2 {
		t.Fatalf("wrong number of in/out params: %d/%d", m.Type.NumIn(), m.Type.NumOut())
	}
	if m.Type.In(0) != reflect.TypeFor[*Conn]() {
		t.Errorf("wrong type for first param")
	}
	if m.Type.NumOut() != 2 {
		t.Fatal("wrong number of out params")
	}
	m0 := m.Type.Out(0)
	if m0.Kind() == reflect.Pointer {
		t.Errorf("first out param should not be a pointer")
	}
	if !m0.Implements(reflect.TypeFor[Message]()) {
		t.Errorf("first out param should implement Message")
	}
	if m0.Name() != "MessageHeader" && !strings.HasSuffix(m0.Name(), "Response") {
		t.Errorf("first out param %q should end with Response", m0.Name())
	}
	if m.Type.Out(1) != reflect.TypeFor[error]() {
		t.Errorf("second out param should be an error")
	}
}

func writeMessage(t *testing.T, b *bytes.Buffer, typ byte, body []byte) {
	t.Helper()
	if err := b.WriteByte(typ); err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(b, binary.BigEndian, int32(len(body)+4)); err != nil {
		t.Fatal(err)
	}
	n, err := b.Write(body)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(body) {
		t.Fatalf("short write: wrote %d bytes, want %d", n, len(body))
	}
}

func writeMessageCString(t *testing.T, b *bytes.Buffer, typ byte, s string) {
	t.Helper()
	var body bytes.Buffer
	n, err := body.WriteString(s)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(s) {
		t.Fatalf("short write: wrote %d bytes, want %d", n, len(s))
	}
	if err := body.WriteByte(0); err != nil {
		t.Fatal(err)
	}
	writeMessage(t, b, typ, body.Bytes())
}

func writeInt16(t *testing.T, b *bytes.Buffer, v int16) {
	t.Helper()
	if err := binary.Write(b, binary.BigEndian, v); err != nil {
		t.Fatal(err)
	}
}

func writeInt32(t *testing.T, b *bytes.Buffer, v int32) {
	t.Helper()
	if err := binary.Write(b, binary.BigEndian, v); err != nil {
		t.Fatal(err)
	}
}

func writeCString(t *testing.T, b *bytes.Buffer, s string) {
	t.Helper()
	n, err := b.WriteString(s)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(s) {
		t.Fatalf("short write: wrote %d bytes, want %d", n, len(s))
	}
	if err := b.WriteByte(0); err != nil {
		t.Fatal(err)
	}
}

func writeReadyForQuery(t *testing.T, b *bytes.Buffer, status ReadyStatus) {
	t.Helper()
	writeMessage(t, b, 'Z', []byte{byte(status)})
}

func writeBackendKeyData(t *testing.T, b *bytes.Buffer, pid, secretKey int32) {
	t.Helper()
	var body bytes.Buffer
	writeInt32(t, &body, pid)
	writeInt32(t, &body, secretKey)
	writeMessage(t, b, 'K', body.Bytes())
}

func writeAuthentication(t *testing.T, b *bytes.Buffer, typ AuthenticationType) {
	t.Helper()
	var body bytes.Buffer
	writeInt32(t, &body, int32(typ))
	writeMessage(t, b, 'R', body.Bytes())
}

func writeDataRow(t *testing.T, b *bytes.Buffer, values ...[]byte) {
	t.Helper()
	var body bytes.Buffer
	writeInt16(t, &body, int16(len(values)))
	for _, v := range values {
		writeInt32(t, &body, int32(len(v)))
		n, err := body.Write(v)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(v) {
			t.Fatalf("short write: wrote %d bytes, want %d", n, len(v))
		}
	}
	writeMessage(t, b, 'D', body.Bytes())
}

func writeNotification(t *testing.T, b *bytes.Buffer, pid int32, channel, payload string) {
	t.Helper()
	var body bytes.Buffer
	writeInt32(t, &body, pid)
	writeCString(t, &body, channel)
	writeCString(t, &body, payload)
	writeMessage(t, b, 'A', body.Bytes())
}

func writeParameterStatus(t *testing.T, b *bytes.Buffer, name, value string) {
	t.Helper()
	var body bytes.Buffer
	writeCString(t, &body, name)
	writeCString(t, &body, value)
	writeMessage(t, b, 'S', body.Bytes())
}
