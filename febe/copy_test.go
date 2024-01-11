package febe

import (
	"bytes"
	"context"
	"testing"
)

func TestCopyData(t *testing.T) {
	var b bytes.Buffer

	// PGCOPY binary preamble
	b.WriteString("PGCOPY\n")
	b.WriteString("\377\r\n\x00")
	b.WriteString("\x00\x00")
	b.WriteString("\x00\x00")

	b.WriteString("\x00\x03") // three columns

	b.WriteString("\x00\x00\x00\x04")
	b.WriteString("\x00\x00\x00\x01") // 1
	b.WriteString("\x00\x00\x00\x04")
	b.WriteString("\x00\x00\x00\x02") // 2
	b.WriteString("\xff\xff\xff\xff") // NULL

	// -1 indicates no more rows
	b.WriteString("\xff\xff\xff\xff")

	var m bytes.Buffer
	m.WriteRune('d')
	m.Write(encodeInt32(4 + b.Len()))
	m.Write(b.Bytes())

	ctx := context.Background()

	cn := New(&m, nil)

	h, err := cn.ReadHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Type() != 'd' {
		t.Fatalf("Type() = %q, want 'd'", h.Type())
	}

	_, err = cn.ReadCopyDataBinaryHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cc, err := cn.ReadCopyDataBinaryColumns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cc.NumCols != 3 {
		t.Fatalf("NumCols = %d, want 3", cc.NumCols)
	}

	for i := range 2 {
		cv, err := cn.ReadCopyDataBinaryValue(ctx)
		if err != nil {
			t.Fatalf("[%d] %v", i, err)
		}

		got := cv.Data
		want := encodeInt32(i + 1)
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadInt32 = %d, want %d", got, want)
		}
	}

	cv, err := cn.ReadCopyDataBinaryValue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cv.Data != nil {
		t.Errorf("Data = %v, want nil", cv.Data)
	}
	if t.Failed() {
		t.FailNow()
	}

	cc, err = cn.ReadCopyDataBinaryColumns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cc.NumCols != -1 {
		t.Fatalf("NumCols = %d, want -1", cc.NumCols)
	}
}

func encodeInt32(i int) []byte {
	return []byte{
		byte(i >> 24),
		byte(i >> 16),
		byte(i >> 8),
		byte(i),
	}
}
