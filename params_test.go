package pqlite

import (
	"iter"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestEncodeParams(t *testing.T) {
	cases := []struct {
		v        any
		wantNull bool
		want     []byte
	}{
		{v: nil, wantNull: true},

		{v: Value{Null: true}, wantNull: true},
		{v: Value{}, wantNull: false},
		{v: Value{Data: []byte{'v'}}, want: []byte{'v'}},
	}

	params := []any{
		nil,

		Value{Null: true},
		Value{Data: []byte{'a'}},

		[]byte(nil), // considered valie (not null)
		[]byte("hello"),
		"world",

		true,
		false,

		uint8(1),
		float32(2),
		float64(3),

		pgEpoch.Add(10 * time.Hour),
	}

	n := make([]byte, 8)
	want := []Value{
		{Null: true},

		{Null: true},
		{Data: []byte{'a'}},

		{Data: nil},
		{Data: []byte("hello")},
		{Data: []byte("world")},

		{Data: []byte{1}},
		{Data: []byte{0}},

		{Data: []byte{1}},
		{Data: putUint32(n, math.Float32bits(2))},
		{Data: putUint64(n, math.Float64bits(3))},

		{Data: putUint64(n, uint64((10*time.Hour)/time.Microsecond))},
	}

	var got []Value
	for v, err := range EncodeParams(params...) {
		if err != nil {
			t.Fatal(err)
		}
		v.Data = slices.Clone(v.Data)
		got = append(got, v)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Error(diff)
	}

	type Unsupported struct{}
	_, err := collect(EncodeParams(Unsupported{}))
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
