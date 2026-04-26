package febe

import (
	"bytes"
	"context"
	"testing"

	"kr.dev/diff"
)

func TestCopyData(t *testing.T) {
	var b bytes.Buffer

	// CopyOut response
	b.WriteString("H\x00\x00\x00\x09\x00\x00\x01\x00\x00")

	// Binary Header
	b.WriteByte('d')
	b.Write([]byte{0, 0, 0, 30})          // Length
	b.WriteString("PGCOPY\n\xff\r\n\x00") // Binary Signature
	b.WriteString("\x00\x00\x00\x00")     // Flags
	b.WriteString("\x00\x00\x00\x00")     // Ext

	b.WriteString("\x00\x01\x00\x00\x00\x01a") // initial data

	b.WriteByte('d')
	b.Write([]byte{0, 0, 0, 12}) // Length
	b.WriteString("\x00\x01\x00\x00\x00\x02bb")

	b.WriteByte('d')
	b.Write([]byte{0, 0, 0, 13}) // Length
	b.WriteString("\x00\x01\x00\x00\x00\x03ccc")

	// CopyDone response
	b.WriteString("c\x00\x00\x00\x04")

	// Error
	b.WriteString("f\x00\x00\x00\x05!\x00")

	ctx := context.Background()

	cn := New(&b, nil)

	_, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	co, err := cn.ReadCopyOut()
	if err != nil {
		t.Fatal(err)
	}
	if co.NumCols != 1 {
		t.Fatalf("NumCols = %d; want %d", co.NumCols, 1)
	}

	var got [][]byte
	for i := range 3 {
		h, err := cn.ReadHeader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if h.Type() != 'd' {
			t.Fatalf("[%d]: Type = %c; want %c", i, h.Type(), 'd')
		}
		dr, err := cn.ReadDataRow(i == 0)
		if err != nil {
			t.Fatalf("[%d]: ReadCopyData() = %v; want nil", i, err)
		}
		if dr.NumCols != 1 {
			t.Fatalf("[%d]: NumCols = %d; want %d", i, dr.NumCols, 1)
		}
		dv, err := cn.ReadDataRowValue()
		if err != nil {
			t.Fatalf("[%d]: ReadDataValue() = %v; want nil", i, err)
		}
		got = append(got, dv.Data)
	}

	diff.Test(t, t.Errorf, got, [][]byte{
		[]byte("a"),
		[]byte("bb"),
		[]byte("ccc"),
	})

	// CopyDone
	h, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Type() != 'c' {
		t.Fatalf("Type = %c; want %c", h.Type(), 'c')
	}

	h, err = cn.ReadHeader(ctx)
	if err == nil {
		t.Fatalf("ReadHeader() = %v; want error", h)
	}
	if w := "postgres: !"; err.Error() != w {
		t.Fatalf("ReadHeader() = %v; want %q", err, w)
	}

	// No more wire data
	h, err = cn.ReadHeader(ctx)
	if err == nil {
		t.Fatalf("ReadHeader() = %v; want error", h)
	}
}

// TestWriteCopyDataRowNull verifies a nil [][]byte entry is sent as the
// PostgreSQL binary-COPY NULL marker (length -1) rather than a zero-length
// non-NULL value.
func TestWriteCopyDataRowNull(t *testing.T) {
	var b bytes.Buffer
	cn := New(nil, &b)

	if err := cn.WriteCopyDataRow([][]byte{nil, []byte("ok"), {}}, false); err != nil {
		t.Fatal(err)
	}
	if err := cn.Flush(); err != nil {
		t.Fatal(err)
	}

	got := b.Bytes()
	want := bytes.Buffer{}
	want.WriteByte('d')
	// length: 4 (self) + 2 (col count) + 4 (col0 len) + 4 (col1 len) + 2 (col1 data) + 4 (col2 len)
	want.Write([]byte{0, 0, 0, 20})
	want.Write([]byte{0, 3})              // 3 columns
	want.Write([]byte{0xff, 0xff, 0xff, 0xff}) // col0: NULL (-1)
	want.Write([]byte{0, 0, 0, 2, 'o', 'k'})   // col1: "ok"
	want.Write([]byte{0, 0, 0, 0})             // col2: zero-length non-NULL

	diff.Test(t, t.Errorf, got, want.Bytes())
}
