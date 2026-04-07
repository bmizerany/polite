package febe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

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

func TestReadHeaderStickyTerminalError(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.c.WriteByte('N')
	cn.c.WriteInt32(4)
	cn.c.Flush()

	ctx := context.Background()
	_, err := cn.ReadHeader(ctx)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadHeader() = %v; want %v", err, io.ErrUnexpectedEOF)
	}
	if !errors.Is(cn.Err(), io.ErrUnexpectedEOF) {
		t.Fatalf("Err() = %v; want %v", cn.Err(), io.ErrUnexpectedEOF)
	}

	// Replace the buffer with a valid message so the next ReadHeader has
	// real data available; if it still returns the sticky error, we know the
	// error is coming from the latched state, not from an empty buffer.
	b.Reset()
	b.WriteByte('Z')
	b.Write([]byte{0, 0, 0, 5})
	b.WriteByte('I')

	_, err = cn.ReadHeader(ctx)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("second ReadHeader() = %v; want %v", err, io.ErrUnexpectedEOF)
	}
	if got := b.Len(); got != 6 {
		t.Fatalf("buffer len after sticky read = %d; want 6", got)
	}
}

func TestReadHeaderDiscard(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.c.WriteByte('D')
	cn.c.WriteInt32(4 + 7)
	cn.c.Write([]byte("DISCARD"))
	cn.c.WriteByte('Z')
	cn.c.WriteInt32(4)
	cn.c.WriteByte('X')
	cn.c.WriteInt32(4)
	cn.c.Flush()

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

func TestTerminatedSticky(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.c.WriteByte('X')
	cn.c.WriteInt32(4)
	cn.c.Flush()

	_, err := cn.ReadHeader(context.Background())
	if !errors.Is(err, ErrTerminated) {
		t.Fatalf("ReadHeader() = %v; want %v", err, ErrTerminated)
	}
	_, err = cn.ReadHeader(context.Background())
	if !errors.Is(err, ErrTerminated) {
		t.Fatalf("ReadHeader() = %v; want %v", err, ErrTerminated)
	}
	err = cn.Flush()
	if !errors.Is(err, ErrTerminated) {
		t.Fatalf("Flush() = %v; want %v", err, ErrTerminated)
	}
	err = cn.WriteSync()
	if !errors.Is(err, ErrTerminated) {
		t.Fatalf("WriteSync() = %v; want %v", err, ErrTerminated)
	}
	err = cn.Err()
	if !errors.Is(err, ErrTerminated) {
		t.Fatalf("Err() = %v; want %v", err, ErrTerminated)
	}
}

func TestFatalErrorSticky(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.WriteNotice('E', &Notice{
		Severity:  "FATAL",
		Code:      "25P03",
		Message:   "terminating connection due to idle-in-transaction timeout",
		Verbosity: "FATAL",
	})
	cn.Flush()

	ctx := context.Background()

	// First ReadHeader should return the FATAL *Error.
	_, err := cn.ReadHeader(ctx)
	var fe *Error
	if !errors.As(err, &fe) {
		t.Fatalf("ReadHeader() = %v; want *febe.Error", err)
	}
	if fe.Code != "25P03" {
		t.Fatalf("error code = %q; want 25P03", fe.Code)
	}
	if !fe.IsFatal() {
		t.Fatalf("IsFatal() = false; want true")
	}

	// Subsequent ReadHeader calls should return the same sticky error.
	_, err = cn.ReadHeader(ctx)
	if !errors.As(err, &fe) || fe.Code != "25P03" {
		t.Fatalf("second ReadHeader() = %v; want sticky FATAL *febe.Error with code 25P03", err)
	}

	// Flush and WriteSync should also return the sticky error.
	if err := cn.Flush(); !errors.As(err, &fe) || fe.Code != "25P03" {
		t.Fatalf("Flush() = %v; want sticky FATAL error", err)
	}
	if err := cn.WriteSync(); !errors.As(err, &fe) || fe.Code != "25P03" {
		t.Fatalf("WriteSync() = %v; want sticky FATAL error", err)
	}
}

func TestPanicErrorSticky(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.WriteNotice('E', &Notice{
		Severity:  "PANIC",
		Code:      "XX000",
		Message:   "could not write to file",
		Verbosity: "PANIC",
	})
	cn.Flush()

	ctx := context.Background()
	_, err := cn.ReadHeader(ctx)
	var fe *Error
	if !errors.As(err, &fe) {
		t.Fatalf("ReadHeader() = %v; want *febe.Error", err)
	}
	if !fe.IsFatal() {
		t.Fatalf("IsFatal() = false; want true for PANIC")
	}

	// Should be sticky
	_, err = cn.ReadHeader(ctx)
	if !errors.As(err, &fe) {
		t.Fatalf("second ReadHeader() = %v; want sticky PANIC *febe.Error", err)
	}
}

func TestErrorSeverityNotSticky(t *testing.T) {
	// ERROR severity should NOT be latched — the connection remains usable.
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.WriteNotice('E', &Notice{
		Severity:  "ERROR",
		Code:      "42601",
		Message:   "syntax error",
		Verbosity: "ERROR",
	})
	cn.Flush()

	// Write a ReadyForQuery + Terminate after the error so the conn has more to read.
	cn.c.WriteByte('Z')
	cn.c.WriteInt32(5)
	cn.c.WriteByte('I')
	cn.c.WriteByte('X')
	cn.c.WriteInt32(4)
	cn.c.Flush()

	ctx := context.Background()

	// First call returns the ERROR.
	_, err := cn.ReadHeader(ctx)
	var fe *Error
	if !errors.As(err, &fe) {
		t.Fatalf("ReadHeader() = %v; want *febe.Error", err)
	}
	if fe.IsFatal() {
		t.Fatalf("IsFatal() = true; want false for ERROR severity")
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

func TestNoticeResponse(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.WriteNotice('N', testNotice)
	cn.Flush()
	cn.c.WriteByte('X') // Terminate
	cn.c.WriteInt32(4)
	cn.c.Flush()

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
	cn.WriteTerminate()
	cn.Flush()

	var got *Notification
	ctx := WithTrace(context.Background(), &Trace{
		ReceivedNotification: func(ctx context.Context, n *Notification) {
			got = n
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
		Payload: []byte("world"),
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
	if err := cn.Err(); err != nil {
		t.Fatal(err)
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
}

func TestReadReadyForQuery(t *testing.T) {
	tests := []struct {
		name   string
		status ReadyStatus
		inTx   bool
	}{
		{name: "idle", status: ReadyStatusIdle},
		{name: "busy", status: ReadyStatusBusy, inTx: true},
		{name: "failed", status: ReadyStatusFailed, inTx: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b bytes.Buffer
			cn := New(&b, &b)
			cn.c.WriteByte('Z')
			cn.c.WriteInt32(5)
			cn.c.WriteByte(byte(tt.status))
			cn.c.Flush()

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
			if got := cn.Status().InTransaction(); got != tt.inTx {
				t.Fatalf("Status().InTransaction() = %v; want %v", got, tt.inTx)
			}
		})
	}
}

func TestReadBackendKeyData(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	defer cn.checkDone(t)

	cn.c.WriteByte('K')
	cn.c.WriteInt32(12)
	cn.c.WriteInt32(1234) // pid
	cn.c.WriteInt32(5678) // secret key
	cn.c.Flush()

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

		cn.c.WriteByte('R')
		cn.c.WriteInt32(8)
		cn.c.WriteInt32(int32(tt.typ))
		cn.c.Flush()

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
	cn.c.WriteByte('C')
	cn.c.WriteInt32(13)
	cn.c.WriteString("SELECT 1")
	cn.c.Flush()

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

// TODO: RowDescription (discarded for now)
// Indicates that rows are about to be returned in response to a SELECT, FETCH,
// etc. query. The contents of this message describe the column layout of the
// rows. This will be followed by a DataRow message for each row being returned
// to the frontend.

// DataRow
// One of the set of rows returned by a SELECT, FETCH, etc. query.
func TestReadDataRow(t *testing.T) {
	ctx := context.Background()

	var b bytes.Buffer
	cn := New(&b, &b)
	defer cn.checkDone(t)

	cn.c.WriteByte('D')
	cn.c.WriteInt32(24)
	cn.c.WriteInt16(2) // number of cols
	cn.c.WriteSizedBytes([]byte("hello"))
	cn.c.WriteSizedBytes([]byte("world"))
	cn.c.Flush()

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
}

func TestReadEmptyQueryResponse(t *testing.T) {
	var b bytes.Buffer
	cn := New(&b, &b)
	cn.c.WriteByte('I')
	cn.c.WriteInt32(4)
	cn.c.Flush()
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
	cn := &Conn{}
	vo := reflect.ValueOf(cn)

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

			vm := vo.Method(i)
			cn.h = MessageHeader{
				msgType: '!',
				msgLen:  1,
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
	defer cn.checkDone(t)

	cn.c.WriteByte('A')
	cn.c.WriteInt32(33)
	cn.c.WriteInt32(1234) // pid
	cn.c.WriteString("hello")
	cn.c.WriteString("world")
	cn.c.Flush()

	if err := cn.readAndSetCurrentHeader(); err != nil {
		t.Fatal(err)
	}

	got, err := cn.readNotification()
	if err != nil {
		t.Fatal(err)
	}

	want := Notification{
		MessageHeader: MessageHeader{
			msgType: 'A',
			msgLen:  33,
		},
		PID:     1234,
		Channel: []byte("hello"),
		Payload: []byte("world"),
	}

	diff.Test(t, t.Errorf, got, want)
}

func TestWriteParse(t *testing.T) {
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
	t.Logf("got:  %q", b.Bytes())
	t.Logf("want: %q", want)
	diff.Test(t, t.Errorf, b.Bytes(), want)
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

	cn.c.WriteByte('S')
	cn.c.WriteInt32(25)
	cn.c.WriteString("server_version")
	cn.c.WriteString("9.6.5")
	cn.c.WriteByte('X')
	cn.c.WriteInt32(4)
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

func (c *Conn) checkDone(t *testing.T) {
	t.Helper()
	_, err := c.c.ReadByte()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("expected io.UnexpectedEOF, got %v", err)
	}
}
