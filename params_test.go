package pqlite

import (
	"iter"
	"reflect"
	"testing"
	"time"
)

func TestEncodeParams(t *testing.T) {
	n := make([]byte, 8)
	cases := []struct {
		p    any
		want Value
	}{
		{nil, Value{Null: true}},
		{Value{Null: true}, Value{Null: true}},
		{[]byte{'a'}, Value{Data: []byte{'a'}}},
		{[]byte(nil), Value{Data: nil}},
		{[]byte("hello"), Value{Data: []byte("hello")}},
		{"world", Value{Data: []byte("world")}},
		{true, Value{Data: []byte{1}}},
		{false, Value{Data: []byte{0}}},
		{uint8(1), Value{Data: []byte{1}}},
		{float32(2), Value{Data: []byte{0x40, 0, 0, 0}}},
		{float64(3), Value{Data: []byte{0x40, 8, 0, 0, 0, 0, 0, 0}}},
		{pgEpoch.Add(4 * time.Hour), Value{Data: putUint64(n, uint64((4*time.Hour)/time.Microsecond))}},
	}

	for i, tt := range cases {
		got, err := EncodeParam(nil, tt.p)
		if err != nil {
			t.Fatalf("[%02d] unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("[%02d] got = %v, want %v", i, got, tt.want)
		}
	}

	type Unsupported struct{}
	_, err := EncodeParam(nil, Unsupported{})
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
