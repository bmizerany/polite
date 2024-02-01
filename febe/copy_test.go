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
	b.WriteString("H\x00\x00\x00\x0b\x01\x00\x02\x00\x01\x00\x01")

	// CopyData response
	b.WriteString("d\x00\x00\x00\x05a")
	b.WriteString("d\x00\x00\x00\x06bb")
	b.WriteString("d\x00\x00\x00\x07ccc")
	b.WriteString("d\x00\x00\x00\x08dddd")
	b.WriteString("c\x00\x00\x00\x04")
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
	if co.NumCols != 2 {
		t.Fatalf("NumCols = %d; want %d", co.NumCols, 2)
	}

	var got [][]byte
	for i := range 4 {
		h, err := cn.ReadHeader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if h.Type() != 'd' {
			t.Fatalf("[%d]: Type = %c; want %c", i, h.Type(), 'd')
		}
		cd, err := cn.ReadCopyData()
		if err != nil {
			t.Fatalf("[%d]: ReadCopyData() = %v; want nil", i, err)
		}
		got = append(got, cd.Data)
	}

	diff.Test(t, t.Errorf, got, [][]byte{
		[]byte("a"),
		[]byte("bb"),
		[]byte("ccc"),
		[]byte("dddd"),
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
