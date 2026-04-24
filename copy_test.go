package polite

import (
	"context"
	"errors"
	"testing"

	"blake.io/polite/febe"
	"kr.dev/diff"
)

func TestCopyFreestyle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)

	defer checkReady(t, cn)

	const q = `
		COPY (SELECT 1 AS id, 'foo' AS name) TO STDOUT WITH (FORMAT binary)
	`
	for row := range cn.Query(ctx, q) {
		g, err := row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		if g != 1 {
			t.Fatalf("id = %v, want 1", g)
		}
		if g, err := row.Column(1).Text(); err != nil {
			t.Fatal(err)
		} else if g != "foo" {
			t.Fatalf("name = %q, want 'foo'", g)
		}
	}
}

func TestCopyNonBinary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)

	defer checkReady(t, cn)

	const q = `
		COPY (SELECT 1) TO STDOUT WITH (FORMAT text)
	`
	var got []error
	for row := range cn.Query(ctx, q) {
		_, err := row.Column(0).Bytes()
		got = append(got, err)
	}
	if len(got) != 1 {
		t.Fatalf("got %v, want 1 error", len(got))
	}
	if g := got[0]; !errors.Is(g, ErrCopyFormat) {
		t.Errorf("got %q, want %q", g, ErrCopyFormat)
	}
}

func TestCopyFromNonBinary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)

	defer checkReady(t, cn)

	err := cn.Exec(ctx, "CREATE TEMP TABLE foo (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	var gotErr error
	for row := range cn.Query(ctx, "COPY foo FROM STDIN") {
		_, err := row.Column(0).Bytes()
		if err != nil {
			gotErr = err
			continue
		}
		t.Fatal("expected an error from Query, got nil")
	}
	if !errors.Is(gotErr, ErrCopyFormat) {
		t.Fatalf("got %v, want ErrCopyFormat", gotErr)
	}
}

func TestDiscardUntilReadyFatal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cn.Close() })

	// The subquery terminates its own backend mid-COPY. The
	// non-binary format causes polite to reject the CopyOutResponse
	// and call drainToReady. While draining, the backend processes
	// the pending SIGTERM and sends a FATAL error.
	//
	// Without the IsFatal guard in drainToReady, this loops forever;
	// with the guard it returns the fatal error.
	var gotErr error
	for row := range cn.Query(ctx, "COPY (SELECT pg_terminate_backend(pg_backend_pid())) TO STDOUT WITH (FORMAT text)") {
		_, gotErr = row.Column(0).Bytes()
	}

	if gotErr == nil {
		t.Fatal("expected an error from Query")
	}
	fe, ok := errors.AsType[*febe.Error](gotErr)
	if !ok {
		t.Fatalf("got %T (%v), want *febe.Error", gotErr, gotErr)
	}
	if !fe.IsFatal() {
		t.Fatalf("got non-fatal error %v, want fatal", fe)
	}
}

func TestCopyFromConstraintViolation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)

	defer checkReady(t, cn)

	err := cn.Exec(ctx, "CREATE TEMP TABLE strict (id INT NOT NULL)")
	if err != nil {
		t.Fatal(err)
	}

	// Enter COPY FROM and send a NULL for a NOT NULL column.
	var gotErr error
	for row := range cn.Query(ctx, "COPY strict FROM STDIN WITH (FORMAT binary)") {
		_, err := row.Column(0).Bytes()
		if err != nil && !errors.Is(err, ErrCopyFrom) {
			gotErr = err
			break
		}
		// Send a row with NULL id — violates NOT NULL.
		if err := cn.Exec(ctx, "", nil); err != nil {
			t.Fatal(err)
		}
	}

	if gotErr == nil {
		t.Fatal("expected a constraint violation error")
	}
	var fe *febe.Error
	if !errors.As(gotErr, &fe) {
		t.Fatalf("got %T (%v), want *febe.Error", gotErr, gotErr)
	}
}

func TestCopyFromBreakAfterConstraintViolation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)
	defer checkReady(t, cn)

	err := cn.Exec(ctx, "CREATE TEMP TABLE strict2 (id INT NOT NULL)")
	if err != nil {
		t.Fatal(err)
	}

	// Send a NULL into a NOT NULL column and break. The constraint
	// violation is discovered during finalization; the connection
	// must still be usable afterward.
	for row := range cn.Query(ctx, "COPY strict2 FROM STDIN WITH (FORMAT binary)") {
		_, err = row.Column(0).Bytes()
		if !errors.Is(err, ErrCopyFrom) {
			t.Fatalf("column error = %v, want ErrCopyFrom", err)
		}
		cn.Exec(ctx, "", nil)
		break
	}

	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("Exec after COPY FROM break with violation: %v", err)
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
			got := collectRows(cn.Query(ctx, tt.sql))
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
	got := collectRows(cn.Query(ctx, q))
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

	for row := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		_, err = row.Column(0).Bytes()
		if !errors.Is(err, ErrCopyFrom) {
			t.Fatalf("column error = %v, want ErrCopyFrom", err)
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
	for row := range cn.Query(ctx, "SELECT * FROM foo ORDER BY id") {
		var g G
		g.ID, err = row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		g.Name, err = row.Column(1).Text()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, g)
	}
	want := []G{
		{ID: 4, Name: "qux"},
		{ID: 5, Name: "quux"},
	}
	diff.Test(t, t.Errorf, got, want)

	checkReady(t, cn)
}

func TestCopyFromBreakBeforeSend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)
	defer checkReady(t, cn)

	err := cn.Exec(ctx, "CREATE TEMP TABLE foo (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	for row := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		_, err = row.Column(0).Bytes()
		if !errors.Is(err, ErrCopyFrom) {
			t.Fatalf("column error = %v, want ErrCopyFrom", err)
		}
		break // exit without sending any rows
	}

	// Connection must be reusable after breaking out of COPY FROM.
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("Exec after COPY FROM break: %v", err)
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

	for row := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		_, err = row.Column(0).Bytes()
		if !errors.Is(err, ErrCopyFrom) {
			t.Fatalf("column error = %v, want ErrCopyFrom", err)
		}
		// Send no rows, just let the iterator end.
	}

	// Verify no rows were inserted.
	var count int64
	for row := range cn.Query(ctx, "SELECT count(*) FROM foo") {
		count, err = row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}

	checkReady(t, cn)
}
