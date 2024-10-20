package pqlite

import (
	"iter"
	"testing"

	"github.com/kr/pretty"
)

func TestEncodeParams(t *testing.T) {
	params := []any{
		nil,
		Value{Null: true},
		Value{Data: []byte{'a'}},
		[]byte(nil), // considered valie (not null)
		[]byte("hello"),
		// pgEpoch.Add(10 * time.Hour),
		// "world",
		// uint8(1),
		// float32(2),
	}

	got, err := collect(EncodeParams(params...))
	if err != nil {
		t.Fatal(err)
	}

	t.Errorf("got:\n% #v", pretty.Formatter(got))

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
