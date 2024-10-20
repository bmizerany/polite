package pqlite

import (
	"iter"
	"testing"
	"time"

	"github.com/kr/pretty"
	"kr.dev/diff"
)

func TestEncodeParams(t *testing.T) {
	params := []any{
		nil,
		Value{Null: true},
		Value{Data: []byte{'a'}},
		[]byte(nil), // considered valie (not null)
		[]byte("hello"),
		"world",
		pgEpoch.Add(10 * time.Hour),
		uint8(1),
		float32(2),
	}

	got, err := collect(EncodeParams(params...))
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("got:\n% #v", pretty.Formatter(got))

	n := make([]byte, 8)
	want := []Value{
		{Null: true},
		{Null: true},
		{Data: []byte{'a'}},
		{Data: nil},
		{Data: []byte("hello")},
		{Data: []byte("world")},
		{Data: putUint64(n, uint64((10*time.Hour)/time.Microsecond))},
		{Data: []byte{1}},
	}
	diff.Test(t, t.Errorf, got, want)

	type Unsupported struct{}
	_, err = collect(EncodeParams(Unsupported{}))
	if err == nil {
		t.Fatal("expected error")
	}
}

func collect[T any](seq iter.Seq2[T, error]) ([]T, error) {
	var vv []T
	for v, err := range seq {
		if err != nil {
			// return vv even if error so we can check it if
			// desired
			return vv, err
		}
		vv = append(vv, v)
	}
	return vv, nil
}
