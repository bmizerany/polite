package postgres

import (
	"context"
	"fmt"
	"testing"

	"blake.io/postgres/febe"
	"go4.org/mem"
	"kr.dev/diff"
)

func TestStream(t *testing.T) {
	dsn := createDB(t)

	ctx := context.Background()
	ctx = febe.WithTrace(ctx, &febe.Trace{
		ReceivedHeader: func(ctx context.Context, h febe.MessageHeader) {
			// t.Logf("received header: %v", h)
		},
	})

	c, err := Dial(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Exec(ctx, "CREATE TABLE foo (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Exec(ctx, "INSERT INTO foo VALUES (1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		t.Fatal(err)
	}

	type G struct {
		ID   int
		Name string
	}

	var got []G
	for row, err := range c.CopyTo(ctx, "foo") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, G{
			// TODO(bmizerany): clean up, this has a lot of
			// punctuation.
			ID:   int(memUint64(must(row.View()))),
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

func memUint64(v mem.RO) uint64 {
	if v.Len() == 4 {
		return uint64(v.At(0))<<24 |
			uint64(v.At(1))<<16 |
			uint64(v.At(2))<<8 |
			uint64(v.At(3))
	}
	if v.Len() == 8 {
		return uint64(v.At(0))<<56 |
			uint64(v.At(1))<<48 |
			uint64(v.At(2))<<40 |
			uint64(v.At(3))<<32 |
			uint64(v.At(4))<<24 |
			uint64(v.At(5))<<16 |
			uint64(v.At(6))<<8 |
			uint64(v.At(7))
	}
	panic(fmt.Errorf("invalid length: %d", v.Len()))
}
