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

var testErrorFields = &ErrorFields{
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

func writeErrorResponse(t *testing.T, EorN byte, c *Conn) {
	t.Helper()

	var s sizer
	s.addInt32()
	testErrorFields.forEachField(func(t byte, v string) {
		s.addByte()
		s.addString(v)
	})
	s.addByte() // terminating byte

	c.c.WriteByte(EorN)
	c.c.WriteInt32(s.size())
	testErrorFields.forEachField(func(t byte, v string) {
		c.c.WriteByte(t)
		c.c.WriteString(v)
	})
	c.c.WriteByte(0)
	c.c.Flush()
}

func TestReadHeaderReadErrorResponse(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)
	writeErrorResponse(t, 'E', c)
	ctx := context.Background()
	gotH, gotErr := c.ReadHeader(ctx)
	diff.Test(t, t.Errorf, gotH, MessageHeader{})
	diff.Test(t, t.Errorf, gotErr, testErrorFields)
}

func TestNoticeResponse(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)
	writeErrorResponse(t, 'N', c)
	want := ErrorResponse{
		MessageHeader: MessageHeader{
			msgType: 'N',
			msgLen:  b.Len() - 1,
		},
		ErrorFields: testErrorFields,
	}

	_, err := c.ReadHeader(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.ReadNotice(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	diff.Test(t, t.Errorf, got, want)
}

func TestWriteStartupMessage(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)
	if err := c.WriteStartupMessage(context.Background(), "user=test"); err != nil {
		t.Fatal(err)
	}
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
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

func TestReadReadyForQuery(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)
	c.c.WriteByte('Z')
	c.c.WriteInt32(5)
	c.c.WriteByte('I')
	c.c.Flush()

	_, err := c.ReadHeader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadReadyForQuery(context.Background())
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

		c.c.WriteByte('R')
		c.c.WriteInt32(8)
		c.c.WriteInt32(int32(tt.typ))
		c.c.Flush()

		_, err := c.ReadHeader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.ReadAuthentication(context.Background())
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
	}
}

func TestReadCommandComplete(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)
	c.c.WriteByte('C')
	c.c.WriteInt32(13)
	c.c.WriteString("SELECT 1")
	c.c.Flush()

	_, err := c.ReadHeader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadCommandComplete(context.Background())
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
	c := New(&b, &b)
	defer c.checkDone(t)

	c.c.WriteByte('D')
	c.c.WriteInt32(24)
	c.c.WriteInt16(2) // number of cols
	c.c.WriteBytes([]byte("hello"))
	c.c.WriteBytes([]byte("world"))
	c.c.Flush()

	_, err := c.ReadHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.ReadDataRow(ctx)
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
		r, err := c.ReadDataRowValue(ctx)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		gotValues[i] = data
	}
	wantValues := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}
	diff.Test(t, t.Errorf, gotValues, wantValues)
}

func TestReadEmptyQueryResponse(t *testing.T) {
	var b bytes.Buffer
	c := New(&b, &b)
	c.c.WriteByte('I')
	c.c.WriteInt32(4)
	c.c.Flush()
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	_, err := c.ReadHeader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadEmptyQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := EmptyQueryResponse{
		MessageHeader: MessageHeader{
			msgType: 'I',
			msgLen:  4,
		},
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
	c := &Conn{}
	vo := reflect.ValueOf(c)
	for i := 0; i < to.NumMethod(); i++ {
		m := to.Method(i)
		if !strings.HasPrefix(m.Name, "Read") {
			continue
		}
		if m.Name == "ReadHeader" {
			// only exception o our rule that ReadX methods must be like:
			//   ReadX(context.Context, MessageHeader) (T, error)
			continue
		}
		t.Run(m.Name, func(t *testing.T) {
			testReadMethodSignature(t, m)

			vm := vo.Method(i)
			ctx := context.Background()
			c.h = MessageHeader{
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
			})
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

var errTest = errors.New("febe_test: test error")

// errorWriter is an io.Writer that always returns an error.
type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("errorWriter.Write: %w", errTest)
}

func testReadMethodSignature(t *testing.T, m reflect.Method) {
	t.Helper()
	if strings.HasSuffix(m.Name, "Response") {
		t.Fatalf("%s should not end with Response", m.Name)
	}
	if m.Type.NumIn() != 2 || m.Type.NumOut() != 2 {
		t.Fatalf("wrong number of in/out params: %d/%d", m.Type.NumIn(), m.Type.NumOut())
	}
	if m.Type.In(0) != reflect.TypeOf((*Conn)(nil)) {
		t.Errorf("wrong type for first param")
	}
	if m.Type.In(1) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		t.Errorf("wrong type for second param")
	}
	if m.Type.NumOut() != 2 {
		t.Fatal("wrong number of out params")
	}
	m0 := m.Type.Out(0)
	if m0.Kind() == reflect.Ptr {
		t.Errorf("first out param should not be a pointer")
	}
	if !m0.Implements(reflect.TypeOf(new(Message)).Elem()) {
		t.Errorf("first out param should implement Message")
	}
	if !strings.HasSuffix(m0.Name(), "Response") {
		t.Errorf("first out param %q should end with Response", m0.Name())
	}
	if m.Type.Out(1) != reflect.TypeOf(new(error)).Elem() {
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

func limitReaders(ss ...string) []*io.LimitedReader {
	lrs := make([]*io.LimitedReader, len(ss))
	for i, s := range ss {
		lrs[i] = &io.LimitedReader{
			N: int64(len(s)),
			R: strings.NewReader(s),
		}
	}
	return lrs
}
