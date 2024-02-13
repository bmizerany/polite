package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go4.org/mem"
	"kr.dev/diff"
)

func TestCopyFreestyle(t *testing.T) {
	ctx := context.Background()
	cn := newSession(t)

	defer func() {
		// Make sure the connection is still alive and in a good state.
		if err := Ping(ctx, cn); err != nil {
			t.Fatal(err)
		}
	}()

	const q = `
		COPY (SELECT 1 AS id, 'foo' AS name) TO STDOUT WITH (FORMAT binary)
	`
	for row, err := range cn.QueryAll(ctx, q) {
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
	cn := newSession(t)

	defer func() {
		// Make sure the connection is still alive and in a good state.
		if err := Ping(ctx, cn); err != nil {
			t.Fatal(err)
		}
	}()

	const q = `
		COPY (SELECT 1) TO STDOUT WITH (FORMAT text)
	`
	var got []error
	for row, err := range cn.QueryAll(ctx, q) {
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

func TestCopyTo(t *testing.T) {
	ctx := context.Background()
	cn := newSession(t)

	for _, q := range []string{
		"CREATE TEMP TABLE foo (id INT, name TEXT)",
		"INSERT INTO foo VALUES (4, 'qux'), (5, 'quux')",
	} {
		err := cn.Exec(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		columns []string
		want    [][]string // values in binary
	}{
		{
			columns: nil,
			want: [][]string{
				{"\x00\x00\x00\x04", "qux"},
				{"\x00\x00\x00\x05", "quux"},
			},
		},
		{
			columns: []string{"id"},
			want: [][]string{
				{"\x00\x00\x00\x04"},
				{"\x00\x00\x00\x05"},
			},
		},
		{
			columns: []string{"name"},
			want: [][]string{
				{"qux"},
				{"quux"},
			},
		},
		{
			columns: []string{"id", "name"},
			want: [][]string{
				{"\x00\x00\x00\x04", "qux"},
				{"\x00\x00\x00\x05", "quux"},
			},
		},
		{
			columns: []string{"name", "id"},
			want: [][]string{
				{"qux", "\x00\x00\x00\x04"},
				{"quux", "\x00\x00\x00\x05"},
			},
		},
	}

	for _, tt := range cases {
		t.Run(fmt.Sprintf("%v", tt.columns), func(t *testing.T) {
			rows, err := cn.CopyTo(ctx, "foo", tt.columns)
			if err != nil {
				t.Fatal(err)
			}
			var got [][]string
			for row, err := range rows.All {
				if err != nil {
					t.Fatal(err)
				}
				var vv []string
				for {
					v, err := row.View()
					if errors.Is(err, ErrOutOfColumns) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("v = %v", v)
					vv = append(vv, v.StringCopy())
				}
				got = append(got, vv)
			}

			diff.Test(t, t.Errorf, got, tt.want)
		})
	}
}

func TestCopyFrom(t *testing.T) {
	ctx := context.Background()
	cn := newSession(t)

	err := cn.Exec(ctx, "CREATE TEMP TABLE foo (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	columns := []string{"id", "name"}
	err = cn.CopyFrom(ctx, "foo", columns, func(yield func([][]byte, error) bool) {
		yield([][]byte{[]byte("\x00\x00\x00\x04"), []byte("qux")}, nil)
		yield([][]byte{[]byte("\x00\x00\x00\x05"), []byte("quux")}, nil)
	})
	if err != nil {
		t.Fatal(err)
	}

	type G struct {
		ID   int
		Name string
	}

	var got []G
	for row, err := range cn.QueryAll(ctx, "SELECT * FROM foo") {
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

func pop[T any, S ~[]T](v *S) T {
	x := (*v)[0]
	*v = (*v)[1:]
	return x
}
