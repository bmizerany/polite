package febe

import (
	"bytes"
	"context"
	"testing"
)

func TestCopyData(t *testing.T) {
	var b bytes.Buffer

	// CopyOut response
	b.WriteString("H\x00\x00\x00\x0b\x01\x00\x02\x00\x01\x00\x01")

	// CopyData response
	b.WriteString("d\x00\x00\x00(PGCOPY\x0a\xff\x0d\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x03foo")
	b.WriteString("d\x00\x00\x00\x15\x00\x02\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00\x03bar")
	b.WriteString("d\x00\x00\x00\x15\x00\x02\x00\x00\x00\x04\x00\x00\x00\x03\x00\x00\x00\x03baz")
	b.WriteString("d\x00\x00\x00\x15\x00\x02\x00\x00\x00\x04\x00\x00\x00\x04\xff\xff\xff\xff")
	b.WriteString("d\x00\x00\x00\x06\xff\xff")
	b.WriteString("c\x00\x00\x00\x04")

	ctx := context.Background()

	cn := New(&b, nil)

	_, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	co, err := cn.ReadCopyOut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if co.NumCols != 2 {
		t.Fatalf("NumCols = %d; want %d", co.NumCols, 2)
	}

	for i, want := range [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		nil,
	} {
		h, err := cn.ReadHeader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if h.Type() != 'd' {
			t.Fatalf("Type = %c; want %c", h.Type(), 'd')
		}
		if i == 0 {
			_, err = cn.ReadCopyDataBinaryHeader(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
		cc, err := cn.ReadCopyDataBinaryNumCols(ctx)
		if cc.NumCols != 2 {
			t.Fatalf("NumCols = %d; want %d", cc.NumCols, 2)
		}
		cv, err := cn.ReadCopyDataBinaryValue(ctx)
		if err != nil {
			t.Fatalf("[%d] %v", i, err)
		}
		if w := []byte{0, 0, 0, byte(i + 1)}; !bytes.Equal(cv.Data, w) {
			t.Fatalf("[%d] Data = %q; want %q", i, cv.Data, w)
		}
		cv, err = cn.ReadCopyDataBinaryValue(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(cv.Data, want) {
			t.Fatalf("[%d] Data = %q; want %q", i, cv.Data, want)
		}
	}

	// No More Data
	h, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Type() != 'd' {
		t.Fatalf("Type = %c; want %c", h.Type(), 'd')
	}
	cc, err := cn.ReadCopyDataBinaryNumCols(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cc.NumCols != -1 {
		t.Fatalf("NumCols = %d; want %d", cc.NumCols, -1)
	}

	// CopyDone
	h, err = cn.ReadHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Type() != 'c' {
		t.Fatalf("Type = %c; want %c", h.Type(), 'c')
	}
}
