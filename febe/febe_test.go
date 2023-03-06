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

func TestErrorResponse(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)

	fields := ErrorFields{
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

	want := ErrorResponse{
		ErrorFields: fields,
	}

	c.WriteErrorResponse(context.Background(), Error, fields)
	c.Flush()

	want.MessageHeader = MessageHeader{
		msgType: 'E',
		msgLen:  b.Len() - 1,
	}

	h, err := c.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.ReadError(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}

	diff.Test(t, t.Errorf, got, want)
}

func BenchmarkWriteErrorResponse(b *testing.B) {
	c := New(nil, io.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := c.WriteErrorResponse(context.Background(), Error, ErrorFields{
			Severity: "ERROR",
			Message:  "MESSAGE",
			Code:     "CODE",
		})
		if err != nil {
			b.Fatal(err)
		}
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
		_, err = c.ReadError(context.Background(), h)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestWriteStartupMessage(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)
	c.WriteStartupMessage(context.Background(), "user", "test")
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		// length of message
		0, 0, 0, 18,
		// protocol version
		0, 3, 0, 0,
		// user
		'u', 's', 'e', 'r', 0,
		't', 'e', 's', 't', 0,
	}
	diff.Test(t, t.Errorf, b.Bytes(), want)
}

func TestReadBackendKeyData(t *testing.T) {
	var b bytes.Buffer
	c := New(nil, &b)
	c.WriteBackendKeyData(context.Background(), 123, 456)
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		// protocol version
		'K',
		// length of message
		0, 0, 0, 12,
		// 123 (int32 big endian)
		0, 0, 0, 123,
		// 456 (int32 big endian)
		0, 0, 1, 200,
	}
	diff.Test(t, t.Errorf, b.Bytes(), want)
}

func TestWriteParameterStatus(t *testing.T) {
	var b bytes.Buffer
	c := New(nil, &b)
	c.WriteParameterStatus(context.Background(), "key", "value")
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		// protocol version
		'S',
		// length of message
		0, 0, 0, 14,
		// key
		'k', 'e', 'y', 0,
		// value
		'v', 'a', 'l', 'u', 'e', 0,
	}
	diff.Test(t, t.Errorf, b.Bytes(), want)
}

func TestWriteReadyForQuery(t *testing.T) {
	var b bytes.Buffer
	c := New(nil, &b)
	c.WriteReadyForQuery(context.Background(), 'I')
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		// protocol version
		'Z',
		// length of message
		0, 0, 0, 5,
		// transaction status
		'I',
	}
	diff.Test(t, t.Errorf, b.Bytes(), want)
}

func TestReadReadyForQuery(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)
	c.WriteReadyForQuery(context.Background(), 'I')
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	h, err := c.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadReadyForQuery(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	want := ReadyForQueryResponse{
		MessageHeader: MessageHeader{
			msgType: 'Z',
			msgLen:  5,
		},
		Status: 'I',
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
		c := New(&b, &b)
		c.WriteAuthentication(context.Background(), tt.typ)
		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
		h, err := c.ReadHeader()
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.ReadAuthentication(context.Background(), h)
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

// CommandComplete
// An SQL command completed normally.
func TestReadCommandComplete(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)
	c.WriteCommandComplete(context.Background(), "SELECT 1")
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	h, err := c.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadCommandComplete(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	want := CommandCompleteResponse{
		MessageHeader: MessageHeader{
			msgType: 'C',
			msgLen:  13,
		},
		Tag: "SELECT 1",
	}
	diff.Test(t, t.Errorf, got, want)
}

// CopyInResponse
// The backend is ready to copy data from the frontend to a table; see Section 55.2.6.

// CopyOutResponse
// The backend is ready to copy data from a table to the frontend; see Section 55.2.6.

// RowDescription
// Indicates that rows are about to be returned in response to a SELECT, FETCH, etc. query. The contents of this message describe the column layout of the rows. This will be followed by a DataRow message for each row being returned to the frontend.

// DataRow
// One of the set of rows returned by a SELECT, FETCH, etc. query.

// EmptyQueryResponse
// An empty query string was recognized.

// ErrorResponse
// An error has occurred.

// ReadyForQuery
// Processing of the query string is complete. A separate message is sent to indicate this because the query string might contain multiple SQL commands. (CommandComplete marks the end of processing one SQL command, not the whole string.) ReadyForQuery will always be sent, whether processing terminates successfully or with an error.

// NoticeResponse
// A warning message has been issued in relation to the query. Notices are in addition to other responses, i.e., the backend will continue processing the command.

func TestNoticeResponse(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)

	fields := ErrorFields{
		Severity:   "NOTICE",
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

	want := ErrorResponse{
		ErrorFields: fields,
	}

	c.WriteErrorResponse(context.Background(), Notice, fields)
	c.Flush()

	want.MessageHeader = MessageHeader{
		msgType: 'N',
		msgLen:  b.Len() - 1,
	}

	h, err := c.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.ReadNotice(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}

	diff.Test(t, t.Errorf, got, want)
}

func TestReadUnknownTypeChecks(t *testing.T) {
	// Using reflect, iterate over all methods on Conn that start with
	// "Read" and match the generic signature:
	//
	//         func(context.Context, MessageHeader) (T, error)
	//
	// For each one found, create a test that calls the method with the
	// MessageHeader.Type set to '!' and and length of the arbitrary value
	// 1. This should cause an error to be returned.

	var checked int

	to := reflect.TypeOf(&Conn{})
	vo := reflect.ValueOf(&Conn{})
	for i := 0; i < to.NumMethod(); i++ {
		m := to.Method(i)
		if m.Name == "ReadHeader" {
			// only exception o our rule that ReadX methods must be like:
			//   ReadX(context.Context, MessageHeader) (T, error)
			continue
		}
		if len(m.Name) < 4 || m.Name[:4] != "Read" {
			continue
		}
		t.Run(m.Name, func(t *testing.T) {
			if strings.HasSuffix(m.Name, "Response") {
				t.Errorf("%s should not end with Response", m.Name)
			}
			if !isReadMethod(t, m) {
				return
			}

		})

		vm := vo.Method(i)
		ctx := context.Background()
		h := MessageHeader{
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
		out := vm.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(h),
		})
		if err := out[1].Interface().(error); err == nil {
			t.Errorf("expected error, got nil")
		}
		checked++
	}

	if checked == 0 {
		t.Fatal("expected to find at least one Read method to check")
	}
}

var errTest = errors.New("febe_test: test error")

// errorWriter is an io.Writer that always returns an error.
type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("errorWriter.Write: %w", errTest)
}

func isReadMethod(t *testing.T, m reflect.Method) bool {
	t.Helper()
	if m.Type.NumIn() != 3 || m.Type.NumOut() != 2 {
		t.Errorf("wrong number of in/out params")
		return false
	}
	if m.Type.In(0) != reflect.TypeOf((*Conn)(nil)) {
		t.Errorf("wrong type for first param")
		return false
	}
	if m.Type.In(1) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		t.Errorf("wrong type for second param")
		return false
	}
	if m.Type.In(2) != reflect.TypeOf((*MessageHeader)(nil)).Elem() {
		t.Errorf("wrong type for third param")
		return false
	}
	if m.Type.NumOut() != 2 {
		t.Errorf("wrong number of out params")
		return false
	}
	m0 := m.Type.Out(0)
	if m0.Kind() == reflect.Ptr {
		t.Errorf("first out param should not be a pointer")
		return false
	}
	if !m0.Implements(reflect.TypeOf(new(Message)).Elem()) {
		t.Errorf("first out param should implement Message")
		return false
	}
	if !strings.HasSuffix(m0.Name(), "Response") {
		t.Errorf("first out param %q should end with Response", m0.Name())
		return false
	}
	if m.Type.Out(1) != reflect.TypeOf(new(error)).Elem() {
		t.Errorf("second out param should be an error")
		return false
	}
	return true
}
