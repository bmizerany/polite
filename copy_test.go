package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

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
		if g := must(row.ParseInt()); g != 1 {
			t.Fatalf("id = %v, want 1", g)
		}
		if g := must(row.ParseString()); g != "foo" {
			t.Fatalf("name = %q, want 'foo'", g)
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
			got := collectRows(t, rows)
			diff.Test(t, t.Errorf, got, tt.want)
		})
	}
}

func TestCopyToReset(t *testing.T) {
	const q = `
		COPY (SELECT 1) TO STDOUT WITH (FORMAT binary)
	`

	ctx := context.Background()
	cn := newSession(t)

	rows, err := cn.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	// Reset the connection to make sure the copy is properly aborted.
	got := collectRows(t, rows)
	want := [][]string{
		{"\x00\x00\x00\x01"},
	}
	diff.Test(t, t.Errorf, got, want)
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
			ID:   must(row.ParseInt()),
			Name: must(row.ParseString()),
		})
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
