package polite

import (
	"context"
	"errors"
	"testing"

	"kr.dev/diff"
)

func TestCopyFreestyle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)

	defer func() {
		// Make sure the connection is still alive and in a good state.
		if err := cn.Exec(ctx, "SELECT 1"); err != nil {
			t.Fatal(err)
		}
	}()

	const q = `
		COPY (SELECT 1 AS id, 'foo' AS name) TO STDOUT WITH (FORMAT binary)
	`
	for row, err := range cn.Query(ctx, q) {
		if err != nil {
			t.Fatal(err)
		}
		g, err := row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		if g != 1 {
			t.Fatalf("id = %v, want 1", g)
		}
		if g := row.Column(1).Text(); g != "foo" {
			t.Fatalf("name = %q, want 'foo'", g)
		}
	}
}

func TestCopyNonBinary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)

	defer func() {
		// ensure recovery
		if err := cn.Exec(ctx, "SELECT 1"); err != nil {
			t.Fatal(err)
		}
	}()

	const q = `
		COPY (SELECT 1) TO STDOUT WITH (FORMAT text)
	`
	var got []error
	for _, err := range cn.Query(ctx, q) {
		got = append(got, err)
	}
	if len(got) != 1 {
		t.Fatalf("got %v, want 1 error", len(got))
	}
	if g := got[0]; !errors.Is(g, ErrUnsupportedCopyFormat) {
		t.Errorf("got %q, want %q", g, ErrUnsupportedCopyFormat)
	}
}

func TestCopyTo(t *testing.T) {
	t.Parallel()

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
		sql  string
		want [][]string
	}{
		{
			sql: "COPY foo TO STDOUT WITH (FORMAT binary)",
			want: [][]string{
				{"\x00\x00\x00\x04", "qux"},
				{"\x00\x00\x00\x05", "quux"},
			},
		},
		{
			sql: `COPY foo (id) TO STDOUT WITH (FORMAT binary)`,
			want: [][]string{
				{"\x00\x00\x00\x04"},
				{"\x00\x00\x00\x05"},
			},
		},
		{
			sql: `COPY foo (name) TO STDOUT WITH (FORMAT binary)`,
			want: [][]string{
				{"qux"},
				{"quux"},
			},
		},
		{
			sql: `COPY foo (id, name) TO STDOUT WITH (FORMAT binary)`,
			want: [][]string{
				{"\x00\x00\x00\x04", "qux"},
				{"\x00\x00\x00\x05", "quux"},
			},
		},
		{
			sql: `COPY foo (name, id) TO STDOUT WITH (FORMAT binary)`,
			want: [][]string{
				{"qux", "\x00\x00\x00\x04"},
				{"quux", "\x00\x00\x00\x05"},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.sql, func(t *testing.T) {
			got := collectRows(t, cn.Query(ctx, tt.sql))
			diff.Test(t, t.Errorf, got, tt.want)
		})
	}
}

func TestCopyToReset(t *testing.T) {
	t.Parallel()

	const q = `
		COPY (SELECT 1) TO STDOUT WITH (FORMAT binary)
	`

	ctx := context.Background()
	cn := newSession(t)

	// Reset the connection to make sure the copy is properly aborted.
	got := collectRows(t, cn.Query(ctx, q))
	want := [][]string{
		{"\x00\x00\x00\x01"},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestCopyFrom(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)

	err := cn.Exec(ctx, "CREATE TEMP TABLE foo (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	for _, err := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		if !errors.Is(err, ErrCopyFrom) {
			t.Fatalf("Query() = %v, want ErrCopyFrom", err)
		}
		if err := cn.Exec(ctx, "", int32(4), "qux"); err != nil {
			t.Fatal(err)
		}
		if err := cn.Exec(ctx, "", int32(5), "quux"); err != nil {
			t.Fatal(err)
		}
	}

	type G struct {
		ID   int64
		Name string
	}

	var got []G
	for row, err := range cn.Query(ctx, "SELECT * FROM foo ORDER BY id") {
		if err != nil {
			t.Fatal(err)
		}
		var g G
		g.ID, err = row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		g.Name = row.Column(1).Text()
		got = append(got, g)
	}
	want := []G{
		{ID: 4, Name: "qux"},
		{ID: 5, Name: "quux"},
	}
	diff.Test(t, t.Errorf, got, want)

	// Make sure the connection is still usable.
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestCopyFromEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)

	err := cn.Exec(ctx, "CREATE TEMP TABLE foo (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	for _, err := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		if !errors.Is(err, ErrCopyFrom) {
			t.Fatalf("Query() = %v, want ErrCopyFrom", err)
		}
		// Send no rows, just let the iterator end.
	}

	// Verify no rows were inserted.
	var count int64
	for row, err := range cn.Query(ctx, "SELECT count(*) FROM foo") {
		if err != nil {
			t.Fatal(err)
		}
		count, err = row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}

	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}
