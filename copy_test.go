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

	defer checkReady(t, cn)

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
	for _, err := range cn.Query(ctx, "COPY foo FROM STDIN") {
		if err != nil {
			gotErr = err
			// A well-behaved caller stops here; Exec must not run.
			continue
		}
		// If we reach here, no error was reported and Exec would
		// run blindly — that's the bug this test guards against.
		t.Fatal("expected an error from Query, got nil")
	}
	if !errors.Is(gotErr, ErrUnsupportedCopyFormat) {
		t.Fatalf("got %v, want ErrUnsupportedCopyFormat", gotErr)
	}
}

func TestDiscardUntilReadyFatal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cn.cn.Close()
	})

	// Create a table with enough rows to generate many CopyData
	// messages during COPY TO, giving us a window to terminate.
	err = cn.Exec(ctx, "CREATE TEMP TABLE big (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	err = cn.Exec(ctx, "INSERT INTO big SELECT generate_series(1, 10000)")
	if err != nil {
		t.Fatal(err)
	}

	// Get the backend PID of the connection under test.
	var pid int64
	for row, err := range cn.Query(ctx, "SELECT pg_backend_pid()") {
		if err != nil {
			t.Fatal(err)
		}
		pid, err = row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Open a second connection to terminate the first.
	killer := newSession(t)

	// Terminate the backend while discardUntilReady is draining the
	// COPY TO stream. The large number of rows ensures there is still
	// data in flight when the terminate arrives.
	killed := make(chan struct{})
	go func() {
		defer close(killed)
		killer.Exec(ctx, "SELECT pg_terminate_backend($1)", int32(pid))
	}()
	// Issue a non-binary COPY TO. The format check rejects it and
	// maybeRecoverFrom calls discardUntilReady to drain the stream.
	// The terminate will cause a FATAL error mid-drain.
	//
	// Without the IsFatal guard in discardUntilReady, this loops
	// forever; with the guard it returns the fatal error.
	var gotErr error
	for _, err := range cn.Query(ctx, "COPY big TO STDOUT WITH (FORMAT text)") {
		gotErr = err
	}

	<-killed

	if gotErr == nil {
		t.Fatal("expected an error from Query")
	}
	var fe *febe.Error
	if !errors.As(gotErr, &fe) {
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
	for row, err := range cn.Query(ctx, "COPY strict FROM STDIN WITH (FORMAT binary)") {
		if err != nil {
			gotErr = err
			break
		}
		if !errors.Is(row.Err(), ErrCopyFrom) {
			t.Fatalf("row.Err() = %v, want ErrCopyFrom", row.Err())
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

	for row, err := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		if err != nil {
			t.Fatal(err)
		}
		if !errors.Is(row.Err(), ErrCopyFrom) {
			t.Fatalf("row.Err() = %v, want ErrCopyFrom", row.Err())
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

	checkReady(t, cn)
}

func TestCopyFromEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cn := newSession(t)

	err := cn.Exec(ctx, "CREATE TEMP TABLE foo (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	for row, err := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		if err != nil {
			t.Fatal(err)
		}
		if !errors.Is(row.Err(), ErrCopyFrom) {
			t.Fatalf("row.Err() = %v, want ErrCopyFrom", row.Err())
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

	checkReady(t, cn)
}
