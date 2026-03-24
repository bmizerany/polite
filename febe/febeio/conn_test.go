package febeio

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

var errTest = errors.New("febeio_test: test error")

type readCase[I comparable] struct {
	in                    string
	want                  I
	err                   error
	skipFollowupReadCheck bool
}

func testRead[T knownTypes](t *testing.T, name string, read func(*Conn) (T, error), cases []readCase[T]) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Helper()
		for i, tt := range cases {
			cn := NewConn(strings.NewReader(tt.in), nil)
			got, err := read(cn)
			if got != tt.want || err != tt.err {
				t.Errorf("[%d]: read(%q) = %v, %v, want %v, %v", i, tt.in, got, err, tt.want, tt.err)
			}
			if !tt.skipFollowupReadCheck {
				_, err = read(cn)
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Errorf("[%d]: read(%q) = %v, want ErrUnexpectedEOF", i, tt.in, err)
				}
			}
		}
	})
}

func TestRead(t *testing.T) {
	testRead(t, "byte", (*Conn).ReadByte, []readCase[byte]{
		{"", 0, io.ErrUnexpectedEOF, false},
		{"\x00", '\x00', nil, false},
		{"\x01", '\x01', nil, false},
		{"\x02", '\x02', nil, false},
		{"T", 'T', nil, false},
	})
	testRead(t, "int16", (*Conn).ReadInt16, []readCase[int16]{
		{"", 0, io.ErrUnexpectedEOF, false},
		{"\x00", 0, io.ErrUnexpectedEOF, false},
		{"\x00\x00", 0, nil, false},
		{"\x00\x01", 1, nil, false},
		{"\x00\x02", 2, nil, false},
		{"\x00\x03", 3, nil, false},
		{"\x00\x04\x01", 4, nil, false},
		{"8\x00\x09", 14336, nil, false},
		{"\xff\xff", -1, nil, false},
		{"\xff\xfe", -2, nil, false},
		{"\xff\xf6", -10, nil, false},
		{"\xfe\xbe", -322, nil, false},
	})
	testRead(t, "int32", (*Conn).ReadInt32, []readCase[int32]{
		{"", 0, io.ErrUnexpectedEOF, false},
		{"\x00", 0, io.ErrUnexpectedEOF, false},
		{"\x00\x00", 0, io.ErrUnexpectedEOF, false},
		{"\x00\x00\x00", 0, io.ErrUnexpectedEOF, false},
		{"\x00\x00\x00\x00", 0, nil, false},
		{"\x00\x00\x00\x01", 1, nil, false},
		{"\x00\x00\x00\x02", 2, nil, false},
		{"\x00\x00\x00\x03", 3, nil, false},
		{"\x00\x00\x00\x04\x01", 4, nil, false},
		{"8\x00\x00\x09", 939524105, nil, false},
		{"\xff\xff\xff\xff", -1, nil, false},
		{"\xff\xff\xff\xfe", -2, nil, false},
		{"\xff\xff\xff\xf6", -10, nil, false},
		{"\xfe\xbe\xfe\xbe", -21037378, nil, false},
	})
	testRead(t, "empty", connReadString, []readCase[string]{
		{"", "", io.ErrUnexpectedEOF, false},
		{"\x00", "", nil, false},
		{"\x01\x00", "\x01", nil, false},
		{"FEBE\x00", "FEBE", nil, false},

		// TODO(bmizerany): decide on a new test once we have a
		// buffer limit in place. Since switch to byteReader we
		// do not have the built-in buffer limit that came with
		// bufio.Reader.
		//
		//
		// fill the buffer without null byte (this should keep testRead
		// from thinking TRAIL\x00 is a valid string on the its
		// follow-up read check)
		// {fullBuffer + "TRAIL\x00", fullBuffer, bufio.ErrBufferFull, true},
	})
}

func TestN(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("H")
	b.Write([]byte{0, 0, 0, 11})
	b.Write([]byte{0, 0, 0, 1})
	b.WriteString("ABC\x00")
	b.WriteString("discard")

	cn := NewConn(&b, nil)

	checkN := func(want int) {
		t.Helper()
		if cn.N() != want {
			t.Errorf("cn.N() = %v, want %v", cn.N(), want)
		}
	}

	cn.ReadByte() // H
	checkN(1)

	cn.ReadByte() // 0
	checkN(2)
	cn.ReadByte() // 0
	checkN(3)

	i16, err := cn.ReadInt16()
	if err != nil {
		t.Errorf("cn.ReadInt16() = _, %v, want nil", err)
	}
	if i16 != 11 {
		t.Errorf("cn.ReadInt16() = %v, want 11", i16)
	}
	checkN(5)

	i32, err := cn.ReadInt32()
	if err != nil {
		t.Errorf("cn.ReadInt32() = _, %v, want nil", err)
	}
	if i32 != 1 {
		t.Errorf("cn.ReadInt32() = %v, want 1", i16)
	}
	checkN(9)

	s, err := cn.ReadString()
	if err != nil {
		t.Errorf("cn.ReadString() = _, %v, want nil", err)
	}
	if string(s) != "ABC" {
		t.Errorf("cn.ReadString() = %v, want ABC", s)
	}
	checkN(13)

	cn.Discard(8)
	checkN(20)

	cn.ResetN()
	checkN(0)
}

func TestErrStickyAfterTruncatedRead(t *testing.T) {
	cn := NewConn(strings.NewReader("\x00"), io.Discard)

	_, err := cn.ReadInt32()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadInt32() = %v; want %v", err, io.ErrUnexpectedEOF)
	}
	err = cn.Err()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Err() = %v; want %v", err, io.ErrUnexpectedEOF)
	}
	err = cn.Flush()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Flush() = %v; want %v", err, io.ErrUnexpectedEOF)
	}
	_, err = cn.ReadByte()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadByte() = %v; want %v", err, io.ErrUnexpectedEOF)
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errTest
}

func TestErrStickyAfterWriteError(t *testing.T) {
	cn := NewConn(strings.NewReader(""), errWriter{})

	if err := cn.WriteByte('x'); err != nil {
		t.Fatalf("WriteByte() = %v; want nil", err)
	}
	if err := cn.Flush(); !errors.Is(err, errTest) {
		t.Fatalf("Flush() = %v; want %v", err, errTest)
	}
	if err := cn.Err(); !errors.Is(err, errTest) {
		t.Fatalf("Err() = %v; want %v", err, errTest)
	}
	if err := cn.Flush(); !errors.Is(err, errTest) {
		t.Fatalf("second Flush() = %v; want %v", err, errTest)
	}
}

func connReadString(c *Conn) (string, error) {
	m, err := c.ReadString()
	return string(m), err
}

type knownTypes interface {
	int16 | int32 | byte | string
}

func testRoundTrip[T knownTypes](t *testing.T, name string, write func(*Conn, T) error, read func(*Conn) (T, error), cases []T) {
	t.Helper()

	var buf bytes.Buffer
	cn := NewConn(&buf, &buf)
	for i, tt := range cases {
		if err := write(cn, tt); err != nil {
			t.Errorf("[%d]: write(%v) = %v, want nil", i, tt, err)
		}
	}
	if err := cn.Flush(); err != nil {
		t.Errorf("flush = %v, want nil", err)
	}
	for i, tt := range cases {
		got, err := read(cn)
		if got != tt || err != nil {
			t.Errorf("[%d]: read = %v, %v, want %v, nil", i, got, err, tt)
		}
		if buf.Len() != 0 {
			t.Errorf("[%d]: buffer not empty", i)
		}
	}
}

func TestRoundTripInt(t *testing.T) {
	testRoundTrip(t, "byte", (*Conn).WriteByte, (*Conn).ReadByte, []byte{
		0,
		'\x01',
		'T',
		'\xff',
		'X',
	})
	testRoundTrip(t, "int16", (*Conn).WriteInt16, (*Conn).ReadInt16, []int16{
		0,
		1,
		2,
		3,
		4,
		14336,
		-1,
		-2,
		-10,
		-322,
	})
	testRoundTrip(t, "int32", (*Conn).WriteInt32, (*Conn).ReadInt32, []int32{
		0,
		1,
		2,
		3,
		4,
		939524105,
		-1,
		-2,
		-10,
		-21037378,
	})
	testRoundTrip(t, "string", (*Conn).WriteString, connReadString, []string{
		"",
		"\x01",
		"FEBE",
		"TRAIL",
	})
}

func benchmarkRead[T knownTypes](b *testing.B, name, in string, want T, read func(*Conn) (T, error)) {
	b.Helper()
	b.Run(name, func(b *testing.B) {
		b.Helper()
		b.ReportAllocs()
		r := strings.NewReader(in)
		cn := NewConn(r, nil)
		for i := 0; i < b.N; i++ {
			r.Reset(in)
			n, err := read(cn)
			if err != nil {
				b.Fatal(err)
			}
			if n != want {
				b.Fatalf("got %v, want %v", n, want)
			}
		}
	})
}

func BenchmarkRead(b *testing.B) {
	benchmarkRead(b, "byte", "\x00", '\x00', (*Conn).ReadByte)
	benchmarkRead(b, "int16", "\x00\x01", 1, (*Conn).ReadInt16)
	benchmarkRead(b, "int32", "\x00\x01\x02\x03", 66051, (*Conn).ReadInt32)
}

func benchmarkWrite[T knownTypes](b *testing.B, name string, v T, write func(*Conn, T) error) {
	b.Helper()
	b.ReportAllocs()
	b.Run(name, func(b *testing.B) {
		b.Helper()
		b.ReportAllocs()
		cn := NewConn(nil, io.Discard)
		for i := 0; i < b.N; i++ {
			if err := write(cn, v); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkWrite(b *testing.B) {
	benchmarkWrite(b, "int16", 1, (*Conn).WriteInt16)
	benchmarkWrite(b, "int32", 66051, (*Conn).WriteInt32)
	benchmarkWrite(b, "byte", 'T', (*Conn).WriteByte)
}

func testWriteAllocs[T knownTypes](t *testing.T, name string, v T, write func(*Conn, T) error) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Helper()
		cn := NewConn(nil, io.Discard)
		n := testing.AllocsPerRun(1000, func() {
			if err := write(cn, v); err != nil {
				t.Fatal(err)
			}
		})
		if n > 0 {
			t.Errorf("got %v allocs, want 0", n)
		}
	})
}

func TestWriteAllocs(t *testing.T) {
	testWriteAllocs(t, "byte", 'T', (*Conn).WriteByte)
	testWriteAllocs(t, "int16", 1, (*Conn).WriteInt16)
	testWriteAllocs(t, "int32", 66051, (*Conn).WriteInt32)
}

func testReadAllocs[T knownTypes](t *testing.T, name, in string, want T, read func(*Conn) (T, error)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Helper()
		r := strings.NewReader("")
		cn := NewConn(r, nil)
		n := testing.AllocsPerRun(100, func() {
			r.Reset(in)
			n, err := read(cn)
			if err != nil {
				t.Fatal(err)
			}
			if n != want {
				t.Fatalf("got %v, want %v", n, want)
			}
		})
		if n > 0 {
			t.Errorf("got %v allocs, want 0", n)
		}
	})
}

func TestReadAllocs(t *testing.T) {
	testReadAllocs(t, "byte", "\x00", '\x00', (*Conn).ReadByte)
	testReadAllocs(t, "int16", "\x00\x01", 1, (*Conn).ReadInt16)
	testReadAllocs(t, "int32", "\x00\x01\x02\x03", 66051, (*Conn).ReadInt32)
	// ReadString is special because it returns a mem.RO so it is tested for
	// allocs seperatly.
}

func TestWriteBytes(t *testing.T) {
	var buf bytes.Buffer
	cn := NewConn(&buf, &buf)
	cn.WriteBytesString("FEBE")
	if err := cn.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := cn.ReadBytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "FEBE" {
		t.Errorf("got = %q, want %q", got, "FEBE")
	}
}

func TestReadByteN(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
		err  error
	}{
		{"", 1, "", io.ErrUnexpectedEOF},
		{"\x00", 2, "", io.ErrUnexpectedEOF}, // ReadFull returns fragment and err
		{"", 0, "", nil},
		{"\x00", 0, "", nil},
		{"FEBE", 4, "FEBE", nil},
	}
	for _, tt := range cases {
		t.Run("", func(t *testing.T) {
			cn := NewConn(strings.NewReader(tt.in), nil)
			got, err := cn.ReadByteN(tt.n)
			if !errors.Is(err, tt.err) {
				t.Errorf("err = %v, want %v", err, tt.err)
			}
			if string(got) != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadByteNAllocs(t *testing.T) {
	r := strings.NewReader("")
	cn := NewConn(r, nil)
	got := testing.AllocsPerRun(100, func() {
		r.Reset("FEBE")
		data, err := cn.ReadByteN(4)
		if err != nil {
			t.Fatal(err)
		}
		if n := len(data); n != 4 {
			t.Fatalf("got %v, want 4", n)
		}
	})
	if got > 0 {
		t.Errorf("got %v allocs, want 0", got)
	}
}

func TestReadStringAllocs(t *testing.T) {
	r := strings.NewReader("")
	cn := NewConn(r, nil)
	got := testing.AllocsPerRun(100, func() {
		r.Reset("FEBE\x00")
		got, err := cn.ReadString()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "FEBE" {
			t.Fatalf("got %v, want FEBE", got)
		}
	})
	if got > 0 {
		t.Errorf("got %v allocs, want 0", got)
	}
}

func TestDiscard(t *testing.T) {
	const size = 10 << 10
	r := strings.NewReader(strings.Repeat("X", size))
	cn := NewConn(r, nil)
	if err := cn.Discard(size); err != nil {
		t.Fatal(err)
	}
	rem, err := cn.ReadByte()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if rem != 0 {
		t.Errorf("rem = %v, want 0", rem)
	}
}
