package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go4.org/mem"
	"kr.dev/diff"
)

func TestCopyFreehand(t *testing.T) {
	ctx := context.Background()
	dsn := createDB(t)
	dsn += " host=localhost" // so we can use Wireshark
	c, err := Dial(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}

	const q = `
		COPY (SELECT 1 AS id, 'foo' AS name) TO STDOUT WITH (FORMAT binary)
	`
	for row, err := range c.QueryAll(ctx, q) {
		if err != nil {
			t.Fatal(err)
		}
		id, err := row.ParseInt()
		if err != nil {
			t.Fatal(err)
		}
		if id != 1 {
			t.Fatalf("id = %v, want 1", id)
		}

		name, err := row.View()
		if err != nil {
			t.Fatal(err)
		}
		if w := "foo"; !name.EqualString(w) {
			t.Fatalf("name = %q, want %q", name.StringCopy(), w)
		}
	}
}

func TestCopyNonBinary(t *testing.T) {
	ctx := context.Background()
	dsn := createDB(t)
	c, err := Dial(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}

	const q = `
		COPY (SELECT 1) TO STDOUT WITH (FORMAT text)
	`
	var got []error
	for row, err := range c.QueryAll(ctx, q) {
		got = append(got, err)
		if row != nil {
			row.Close()
		}
	}
	if len(got) != 1 {
		t.Fatalf("got %v, want 1 error", len(got))
	}
	const want = "unsupported COPY format"
	if g := got[0]; !strings.Contains(g.Error(), want) {
		t.Fatalf("got %q, want %q", g, want)
	}
}

func TestStream(t *testing.T) {
	dsn := createDB(t)

	ctx := context.Background()
	c, err := Dial(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Exec(ctx, "CREATE TABLE foo (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	err = c.Exec(ctx, "INSERT INTO foo VALUES (1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		t.Fatal(err)
	}

	type G struct {
		ID   int
		Name string
	}

	var got []G
	for row, err := range c.CopyTo(ctx, "foo").All {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, G{
			// TODO(bmizerany): clean up, this has a lot of
			// punctuation.
			ID:   int(memUint(must(row.View()))),
			Name: must(row.View()).StringCopy(),
		})
	}

	want := []G{
		{ID: 1, Name: "foo"},
		{ID: 2, Name: "bar"},
		{ID: 3, Name: "baz"},
	}

	diff.Test(t, t.Errorf, got, want)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func memUint(v mem.RO) uint {
	if v.Len() == 2 {
		return uint(uint16(v.At(1)) | uint16(v.At(0))<<8)
	}
	if v.Len() == 4 {
		return uint(uint32(v.At(3)) | uint32(v.At(2))<<8 | uint32(v.At(1))<<16 | uint32(v.At(0))<<24)
	}
	if v.Len() == 8 {
		return uint(uint64(v.At(7)) | uint64(v.At(6))<<8 | uint64(v.At(5))<<16 | uint64(v.At(4))<<24 | uint64(v.At(3))<<32 | uint64(v.At(2))<<40 | uint64(v.At(1))<<48 | uint64(v.At(0))<<56)
	}
	panic(fmt.Errorf("invalid length: %d", v.Len()))
}
