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

func TestReadCopyBoth(t *testing.T) {
	var b bytes.Buffer

	// CopyBothResponse: type 'W', format binary, 2 columns
	b.WriteByte('W')
	b.Write([]byte{0, 0, 0, 11})  // length: 4 + 1 + 2 + 2*2 = 11
	b.WriteByte(1)                 // format: binary
	b.Write([]byte{0, 2})         // numCols: 2
	b.Write([]byte{0, 1, 0, 1})   // format codes (ignored)

	ctx := context.Background()
	cn := New(&b, nil)

	_, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cr, err := cn.ReadCopyBoth()
	if err != nil {
		t.Fatal(err)
	}
	if cr.Format != CopyFormatBinary {
		t.Fatalf("Format = %v; want binary", cr.Format)
	}
	if cr.NumCols != 2 {
		t.Fatalf("NumCols = %d; want 2", cr.NumCols)
	}
}
