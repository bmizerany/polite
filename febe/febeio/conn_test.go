package febeio

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"go4.org/mem"
)

var fullBuffer = strings.Repeat("F", 4096)

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
			c := NewConn(strings.NewReader(tt.in), nil)
			got, err := read(c)
			if got != tt.want || err != tt.err {
				t.Errorf("[%d]: read(%q) = %v, %v, want %v, %v", i, tt.in, got, err, tt.want, tt.err)
			}
			if !tt.skipFollowupReadCheck {
				_, err = read(c)
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

		// fill the buffer without null byte (this should keep testRead
		// from thinking TRAIL\x00 is a valid string on the its
		// follow-up read check)
		{fullBuffer + "TRAIL\x00", fullBuffer, bufio.ErrBufferFull, true},
	})
}

func connReadString(c *Conn) (string, error) {
	m, err := c.ReadString()
	return m.StringCopy(), err
}

func connWriteString(c *Conn, s string) error {
	return c.WriteString(mem.S(s))
}

type knownTypes interface {
	int16 | int32 | byte | string
}

func testRoundTrip[T knownTypes](t *testing.T, name string, write func(*Conn, T) error, read func(*Conn) (T, error), cases []T) {
	t.Helper()

	var buf bytes.Buffer
	c := NewConn(&buf, &buf)
	for i, tt := range cases {
		if err := write(c, tt); err != nil {
			t.Errorf("[%d]: write(%v) = %v, want nil", i, tt, err)
		}
	}
	if err := c.Flush(); err != nil {
		t.Errorf("flush = %v, want nil", err)
	}
	for i, tt := range cases {
		got, err := read(c)
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
	testRoundTrip(t, "string", connWriteString, connReadString, []string{
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
		c := NewConn(r, nil)
		for i := 0; i < b.N; i++ {
			r.Reset(in)
			n, err := read(c)
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
		c := NewConn(nil, io.Discard)
		for i := 0; i < b.N; i++ {
			if err := write(c, v); err != nil {
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
		c := NewConn(nil, io.Discard)
		n := testing.AllocsPerRun(1000, func() {
			if err := write(c, v); err != nil {
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
		c := NewConn(r, nil)
		n := testing.AllocsPerRun(100, func() {
			r.Reset(in)
			n, err := read(c)
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

func TestReadByteN(t *testing.T) {
	cases := []struct {
		in  string
		n   int
		out string
		err error
	}{
		{"", 1, "", io.ErrUnexpectedEOF},
		{"\x00", 2, "\x00", io.ErrUnexpectedEOF}, // ReadFull returns fragment and err
		{"", 0, "", nil},
		{"\x00", 0, "", nil},
		{"FEBE", 4, "FEBE", nil},
	}
	for _, tt := range cases {
		t.Run("", func(t *testing.T) {
			c := NewConn(strings.NewReader(tt.in), nil)
			r := c.ReadByteN(tt.n)
			got, err := io.ReadAll(r)
			if !errors.Is(err, tt.err) {
				t.Errorf("err = %v, want %v", err, tt.err)
			}
			if string(got) != tt.out {
				t.Errorf("got %q, want %q", got, tt.out)
			}
		})
	}
}

func TestReadByteNDiscardOnClose(t *testing.T) {
	in := strings.NewReader("abc\x00\x09") // "abc" + int16(9)
	c := NewConn(in, nil)

	var b [1]byte
	r := c.ReadByteN(3)
	n, err := r.Read(b[:])
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("n = %v, want 1", n)
	}
	if b[0] != 'a' {
		t.Fatalf("b[0] = %v, want 'a'", b[0])
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	g, err := c.ReadInt16()
	if err != nil {
		t.Fatal(err)
	}
	if g != 9 {
		t.Errorf("got = %v, want 9", g)
	}
	if in.Len() != 0 {
		t.Errorf("r.Len() = %v, want 0", in.Len())
	}
}

func TestReadByteNMultipleCloses(t *testing.T) {
	c := NewConn(strings.NewReader("abcXYZ\x00"), nil)
	r := c.ReadByteN(3)

	// Close the reader, multiple times, safely.
	r.Close()
	r.Close()
	r.Close()

	// Make sure the reader is still usable and we can read the rest of the
	// data.
	s, err := c.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if g := s.StringCopy(); g != "XYZ" {
		t.Errorf("got = %q, want XYZ", g)
	}
}

func TestReadByteNAllocs(t *testing.T) {
	r := strings.NewReader("")
	c := NewConn(r, nil)
	got := testing.AllocsPerRun(100, func() {
		r.Reset("FEBE")
		r := c.ReadByteN(4)
		n, err := io.Copy(io.Discard, r)
		if err != nil {
			t.Fatal(err)
		}
		if n != 4 {
			t.Fatalf("got %v, want 4", n)
		}
	})
	if got > 0 {
		t.Errorf("got %v allocs, want 0", got)
	}
}

func TestReadStringAllocs(t *testing.T) {
	r := strings.NewReader("")
	c := NewConn(r, nil)
	got := testing.AllocsPerRun(100, func() {
		r.Reset("FEBE\x00")
		got, err := c.ReadString()
		if err != nil {
			t.Fatal(err)
		}
		if !got.Equal(mem.S("FEBE")) {
			t.Fatalf("got %v, want FEBE", got.StringCopy())
		}
	})
	if got > 0 {
		t.Errorf("got %v allocs, want 0", got)
	}
}
