package polite

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"net"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"blake.io/polite/febe"
	"blake.io/polite/internal/testutil"
	"blake.io/polite/oids"
	"kr.dev/diff"
)

// This is added for tests that need to check error handling. It comes
// standard with all sessions created with newSession.
const (
	sqlCreateRaiseNotice = `
		CREATE OR REPLACE FUNCTION raise_notice() RETURNS integer
		   LANGUAGE plpgsql AS
		$$BEGIN
		   RAISE NOTICE 'boom notice';
		   RETURN 42;
		END;$$;
	`
	sqlCreateRaiseError = `
		CREATE OR REPLACE FUNCTION raise_error() RETURNS integer
		   LANGUAGE plpgsql AS
		$$BEGIN
		   RAISE EXCEPTION 'boom error';
		   RETURN 42;
		END;$$;
	`
)

// createDB creates a new database for the test using its name as the
// prefix. It returns the DSN for the new database.
func createDB(t testing.TB) string {
	t.Helper()

	cn, err := Dial(t.Context(), testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}

	tname := strings.ReplaceAll(t.Name(), "/", "_")
	dbname := fmt.Sprintf("gotest_%s_%s", tname, rand.Text()[:6])
	dbname = strings.ToLower(dbname)
	if err := cn.Exec(t.Context(), "CREATE DATABASE "+dbname); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(testDSN)
	if err == nil && u.Scheme != "" {
		u2 := *u
		u2.Path = "/" + dbname
		dsn := u2.String()
		t.Logf("newConn: dsn: %q", dsn)
		return dsn
	}

	dsn := testDSN + " dbname=" + dbname
	t.Logf("newConn: dsn: %q", dsn)
	return dsn
}

// newSessionDB opens and returns a database connection to mainDSN.
//
// Users that need a clean database should use createDB instead.
func newSessionDB(t testing.TB) *Conn {
	ctx := t.Context()
	dsn := createDB(t)
	if t.Failed() {
		t.FailNow()
	}

	cn, err := Dial(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cn.Close() })
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	return cn
}

var funcsCreated atomic.Bool

func newSession(t testing.TB) *Conn {
	ctx := t.Context()
	cn, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cn.Close() })

	if err := cn.Exec(ctx, "SET TimeZone TO 'UTC'"); err != nil {
		t.Fatal(err)
	}

	if funcsCreated.Swap(true) == true {
		return cn
	}

	for _, sql := range []string{
		sqlCreateRaiseNotice,
		sqlCreateRaiseError,
	} {
		if err := cn.Exec(t.Context(), sql); err != nil {
			t.Fatal(err)
		}
	}
	return cn
}

// TestExecInsideQueryRangeErrors verifies that calling Conn.Exec
// inside an active Conn.Query range (outside COPY) returns
// ErrBusy and leaves the connection usable.
func TestExecInsideQueryRangeErrors(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cn := newSession(t)

	const q = `SELECT 1 UNION ALL SELECT 2`
	var innerExecErr error
	rowsSeen := 0
	for row := range cn.Query(ctx, q) {
		rowsSeen++
		if _, err := row.Column(0).DecodeInt(); err != nil {
			t.Fatalf("decode row %d: %v", rowsSeen, err)
		}
		if innerExecErr == nil {
			innerExecErr = cn.Exec(ctx, "SELECT 1")
		}
	}

	if !errors.Is(innerExecErr, ErrBusy) {
		t.Fatalf("Exec inside Query: got %v; want ErrBusy", innerExecErr)
	}
	if rowsSeen != 2 {
		t.Errorf("rowsSeen = %d; want 2 (outer iterator should complete cleanly)", rowsSeen)
	}
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Errorf("Exec after Query loop: %v; want nil (connection should be usable)", err)
	}
}

func TestQuery(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	for range 3 {
		const q = `SELECT 1, 2, 'three'`
		var got [][]any
		for row := range cn.Query(t.Context(), q) {
			v0, err := row.Column(0).DecodeInt()
			if err != nil {
				t.Fatal(err)
			}
			v1, err := row.Column(1).DecodeInt()
			if err != nil {
				t.Fatal(err)
			}
			v2, err := row.Column(2).DecodeText()
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, []any{
				v0,
				v1,
				v2,
			})
		}
		want := [][]any{{int64(1), int64(2), "three"}}
		diff.Test(t, t.Errorf, got, want)
	}
}

// TestQueryAllocations asserts Query is allocation-free in steady
// state, both with and without a cancellable ctx. The with-cancel
// variant exercises the cancel watcher path (long-lived cancelLoop
// goroutine in production mode); the without-cancel variant exercises
// the fast path that skips the watcher entirely. Both must stay at
// zero — any regression points at either the watcher or row decoding.
func TestQueryAllocations(t *testing.T) {
	check := func(t *testing.T, ctx context.Context) {
		t.Helper()
		cn := newSession(t)
		allocs := testing.AllocsPerRun(1000, func() {
			const q = `SELECT 1, 'two'`
			for row := range cn.Query(ctx, q) {
				if _, err := row.Column(0).DecodeInt(); err != nil {
					t.Fatal(err)
				}
				if _, err := row.Column(1).DecodeText(); err != nil {
					t.Fatal(err)
				}
			}
		})
		if allocs > 0 {
			t.Errorf("unexpected allocations: %.2f", allocs)
		}
	}

	t.Run("with_cancel", func(t *testing.T) { check(t, t.Context()) })
	t.Run("without_cancel", func(t *testing.T) { check(t, context.Background()) })
}

func BenchmarkQueryAllocations(b *testing.B) {
	b.ReportAllocs()

	cn := newSession(b)
	ctx := b.Context()

	const q = `SELECT 1, 'two'`

	cn.Exec(ctx, q) // warmup

	for b.Loop() {
		for row := range cn.Query(ctx, q) {
			if _, err := row.Column(0).DecodeInt(); err != nil {
				b.Fatal(err)
			}
			if _, err := row.Column(1).Bytes(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestDeadlineIsolation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}

	cancel()

	if err := c.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("unexpected error after context cancel: %v", err)
	}
}

func TestConnClose(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	cn.Close()
	cn.Close()

	// Ensure we can't use the connection after closing.
	if err := cn.Exec(t.Context(), "SELECT 1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("query(SELECT 1) = %v; want %v", err, ErrClosed)
	}
}

type failWriteConn struct {
	net.Conn

	failWrites bool
}

func (c *failWriteConn) Write(p []byte) (int, error) {
	if c.failWrites {
		return 0, io.ErrClosedPipe
	}
	return c.Conn.Write(p)
}

func TestConnCloseUnderlyingConnDespiteFlushError(t *testing.T) {
	t.Parallel()

	var dialer net.Dialer
	raw := new(failWriteConn)
	cn, err := Dial(t.Context(), testDSN, &Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialer.DialContext(ctx, network, addr)
			raw.Conn = c
			return raw, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// After startup, fail writes locally so Terminate/Flush trips. Close must
	// still close the real transport underneath the wrapper.
	raw.failWrites = true
	if err := cn.Close(); err != nil {
		t.Fatalf("Close() = %v; want nil", err)
	}
	if _, err := raw.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("raw.Read() = %v; want %v", err, net.ErrClosed)
	}
}

// TestConnIdle walks a single connection through the full spectrum of
// observable states and pins Idle()'s contract at each step: true only when
// the connection is ready for the next query, false in every other state.
// The terminal backend kill is last so the whole story runs on one conn.
func TestConnIdle(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	ctx := t.Context()

	checkIdle := func(want bool) {
		t.Helper()
		if got := cn.Idle(); got != want {
			t.Fatalf("Idle() = %v; want %v", got, want)
		}
	}

	exec := func(q string) {
		t.Helper()
		if err := cn.Exec(ctx, q); err != nil {
			t.Fatalf("Exec(%q) = %v; want nil", q, err)
		}
	}

	// A freshly dialed session is idle.
	checkIdle(true)

	// A completed Exec returns the conn to idle.
	exec("SELECT 1")
	checkIdle(true)

	// While a Query range owns the conn it is not idle, but the range
	// completing returns it to idle.
	rows := 0
	for row := range cn.Query(ctx, "SELECT generate_series(1, 3)") {
		if _, err := row.Column(0).DecodeInt(); err != nil {
			t.Fatal(err)
		}
		checkIdle(false)
		rows++
	}
	if rows != 3 {
		t.Fatalf("rows = %d; want 3", rows)
	}
	checkIdle(true)

	// Inside a transaction: not idle.
	exec("BEGIN")
	checkIdle(false)

	// Failed statement inside the tx: still not idle (failed-tx state).
	if err := cn.Exec(ctx, "SELECT 1/0"); err == nil {
		t.Fatal("SELECT 1/0 = nil; want error")
	}
	checkIdle(false)

	// ROLLBACK clears the tx and returns to idle.
	if err := cn.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	checkIdle(true)

	// Mid COPY FROM: inCopyFrom is set — not idle.
	exec("CREATE TEMP TABLE foo (id INT)")
	for row := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		if _, err := row.Column(0).Bytes(); !errors.Is(err, ErrCopyFrom) {
			t.Fatalf("COPY FROM: got %v; want ErrCopyFrom", err)
		}
		checkIdle(false)
		break
	}

	// Finally, terminate our own backend. Done last so the whole test
	// stays on one conn; any subsequent op returns a sticky terminal error
	// and the conn is no longer idle.
	if err := cn.Exec(ctx, "SELECT pg_terminate_backend(pg_backend_pid())"); err == nil {
		t.Fatal("terminate = nil; want error")
	}
	checkIdle(false)
}

func TestRowClose(t *testing.T) {
	t.Parallel()

	const q = `SELECT 1, 2`

	cn := newSession(t)

	t.Run("with remaining values", func(t *testing.T) {
		for range cn.Query(t.Context(), q) {
		}
	})

	t.Run("without remaining values", func(t *testing.T) {
		for range cn.Query(t.Context(), q) {
		}
	})
}

func TestQueryExtended(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	const q = `
		SELECT $1
		UNION ALL
		SELECT $2
	`
	var got []string
	for row := range cn.Query(t.Context(), q, "hello", "world") {
		v, err := row.Column(0).DecodeText()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}

	want := []string{"hello", "world"}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryExtendedBytes(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	var got []string
	for row := range cn.Query(t.Context(), "SELECT $1::bytea", []byte{0x12, 0x34, 0x35}) {
		b, err := row.Column(0).Bytes()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(b))
	}

	diff.Test(t, t.Errorf, got, []string{"\x12\x345"})
}

func TestQueryExtendedNullBytes(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	for row := range cn.Query(t.Context(), "SELECT $1::bytea", nil) {
		got, err := row.Column(0).Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("unexpected non-nil got: %v", got)
		}
		return
	}

	t.Fatal("expected 1 row")
}

func TestQueryExtendedUnsupportedType(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	t.Cleanup(func() { checkReady(t, cn) })

	var got []error
	for row := range cn.Query(t.Context(), "SELECT $1", true) {
		_, err := row.Column(0).Bytes()
		got = append(got, err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 error; got %d", len(got))
	}
	if got[0] == nil || !strings.Contains(got[0].Error(), "polite:") {
		t.Fatalf("expected polite error; got %v", got[0])
	}
}

func TestQueryExtendedComplex(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	t.Cleanup(func() { checkReady(t, cn) })

	var got []error
	for row := range cn.Query(t.Context(), "SELECT $1", complex128(1+2i)) {
		_, err := row.Column(0).Bytes()
		got = append(got, err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 error; got %d", len(got))
	}
	if got[0] == nil || !strings.Contains(got[0].Error(), "polite:") {
		t.Fatalf("expected polite error; got %v", got[0])
	}
}

func collectRows(rows iter.Seq[*Row]) [][]string {
	var got [][]string
	for row := range rows {
		got = append(got, collectValues(row))
	}
	return got
}

func collectValues(row *Row) []string {
	var got []string
	for col := range row.Columns() {
		b, _ := col.Bytes()
		got = append(got, string(b))
	}
	return got
}

func TestRowColumn(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	t.Run("basic", func(t *testing.T) {
		for row := range cn.Query(t.Context(), `SELECT 11::int4, 'two'::text`) {

			col := row.Column(1)
			s, err := col.DecodeText()
			if err != nil {
				t.Fatal(err)
			}
			if s != "two" {
				t.Fatalf("column 1 = %q, want %q", s, "two")
			}

			col = row.Column(0)
			n, err := col.DecodeInt()
			if err != nil {
				t.Fatal(err)
			}
			if n != 11 {
				t.Fatalf("column 0 = %v, want 11", n)
			}

			col = row.Column(0)
			n, err = col.DecodeInt()
			if err != nil {
				t.Fatal(err)
			}
			if n != 11 {
				t.Fatalf("column 0 repeated = %v, want 11", n)
			}
			return
		}
		t.Fatal("expected 1 row")
	})

	t.Run("negative_and_zero", func(t *testing.T) {
		for row := range cn.Query(t.Context(), `SELECT -7::int2, 0::int2, -123456::int4, 0::int4, -1234567890123::int8, 0::int8`) {

			var got []int64
			for col := range row.Columns() {
				n, err := col.DecodeInt()
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, n)
			}

			want := []int64{-7, 0, -123456, 0, -1234567890123, 0}
			diff.Test(t, t.Errorf, got, want)
			return
		}
		t.Fatal("expected 1 signed integer row")
	})
}

func TestRowColumnPanic(t *testing.T) {
	t.Parallel()

	t.Run("column_index", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			index     int
			wantPanic bool
		}{
			{name: "negative_index", index: -1, wantPanic: true},
			{name: "zero", index: 0},
			{name: "past_end", index: 1, wantPanic: true},
		} {
			func() {
				cn := newSession(t)
				if tc.wantPanic {
					defer func() {
						if r := recover(); r == nil {
							t.Fatalf("expected %s panic", tc.name)
						}
					}()
				}

				for row := range cn.Query(t.Context(), `SELECT 11::int4`) {
					row.Column(tc.index)
					if !tc.wantPanic {
						continue
					}
					t.Fatalf("expected %s panic", tc.name)
				}

				if tc.wantPanic {
					t.Fatal("expected 1 row")
				}
			}()
		}
	})
}

func TestColumnOrdinal(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	for row := range cn.Query(t.Context(), `SELECT 11::int4 AS one, 22::int4 AS two`) {
		if got := row.Column(0).Ordinal(); got != 0 {
			t.Fatalf("Column(0).Ordinal() = %d, want 0", got)
		}
		if got := row.Column(1).Ordinal(); got != 1 {
			t.Fatalf("Column(1).Ordinal() = %d, want 1", got)
		}
		return
	}
	t.Fatal("expected 1 row")
}

func TestZeroColumn(t *testing.T) {
	t.Parallel()

	var col Column
	if got, err := col.Bytes(); err != nil {
		t.Fatalf("Bytes() error = %v; want nil", err)
	} else if got != nil {
		t.Fatalf("Bytes() = %v; want nil", got)
	}
	if got := col.Ordinal(); got != 0 {
		t.Fatalf("Ordinal() = %d; want 0", got)
	}
	if got := col.Name(); got != "" {
		t.Fatalf("Name = %q; want empty", got)
	}
	if got := col.Size(); got != 0 {
		t.Fatalf("Size = %d; want 0", got)
	}
	if got := col.Type(); got != 0 {
		t.Fatalf("Type = %v; want 0", got)
	}
}

// testDecodeNull is the body of TestDecodeNull's subtests. It runs
// sql against cn, decodes column 0 with decode, and asserts both
// the typed return equals want and the error matches wantErrSubstr
// (empty wantErrSubstr means err must be nil).
func testDecodeNull[T comparable](t *testing.T, cn *Conn, sql string, decode func(Column) (T, error), want T, wantErrSubstr string) {
	t.Helper()
	for row := range cn.Query(t.Context(), sql) {
		got, err := decode(row.Column(0))
		if wantErrSubstr == "" {
			if err != nil {
				t.Errorf("err = %v; want nil", err)
			}
		} else {
			if err == nil {
				t.Errorf("err = nil; want substring %q", wantErrSubstr)
			} else if !strings.Contains(err.Error(), wantErrSubstr) {
				t.Errorf("err = %q; want substring %q", err, wantErrSubstr)
			}
		}
		if got != want {
			t.Errorf("got %v; want %v", got, want)
		}
		return
	}
	t.Fatal("expected 1 row")
}

// TestDecodeNull pins what each Column reader yields on a SQL NULL.
// Bytes returns nil; Text returns "" (indistinguishable from an
// empty-string column); the typed decoders return their zero values
// with an "invalid length" error, since their return types cannot
// represent missing.
func TestDecodeNull(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	// Bytes returns []byte, which is not a comparable type, so it
	// does not fit the testDecodeNull helper.
	t.Run("Bytes", func(t *testing.T) {
		for row := range cn.Query(t.Context(), `SELECT NULL::bytea`) {
			got, err := row.Column(0).Bytes()
			if err != nil {
				t.Errorf("err = %v; want nil", err)
			}
			if got != nil {
				t.Errorf("got %v; want nil", got)
			}
			return
		}
		t.Fatal("expected 1 row")
	})
	t.Run("Text", func(t *testing.T) {
		testDecodeNull(t, cn, `SELECT NULL::text`, Column.DecodeText, "", "")
	})
	t.Run("DecodeInt", func(t *testing.T) {
		testDecodeNull(t, cn, `SELECT NULL::int4`, Column.DecodeInt, int64(0), "invalid length")
	})
	t.Run("DecodeFloat", func(t *testing.T) {
		testDecodeNull(t, cn, `SELECT NULL::float8`, Column.DecodeFloat, float64(0), "invalid length")
	})
	t.Run("DecodeBool", func(t *testing.T) {
		testDecodeNull(t, cn, `SELECT NULL::bool`, Column.DecodeBool, false, "invalid length")
	})
	t.Run("DecodeTime", func(t *testing.T) {
		testDecodeNull(t, cn, `SELECT NULL::timestamptz`, Column.DecodeTime, time.Time{}, "invalid length")
	})
}

func TestQueryAll(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	var got [][]string
	for row := range cn.Query(t.Context(), "SELECT * FROM generate_series(1, 5)") {
		got = append(got, collectValues(row))
	}
	want := [][]string{
		{"\x00\x00\x00\x01"},
		{"\x00\x00\x00\x02"},
		{"\x00\x00\x00\x03"},
		{"\x00\x00\x00\x04"},
		{"\x00\x00\x00\x05"},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryBreakInAll(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	t.Cleanup(func() { checkReady(t, cn) })
	var got [][]string
	for row := range cn.Query(t.Context(), "SELECT * FROM generate_series(1, 5)") {
		got = append(got, collectValues(row))
		break
	}
	want := [][]string{
		{"\x00\x00\x00\x01"},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryBreakDrainError(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	// generate_series produces rows 1..5. The division g/(3-g)
	// raises a division-by-zero error when g reaches 3. Breaking
	// after the first row forces the drain loop to encounter the
	// error while discarding the remaining rows.
	const q = `SELECT CASE WHEN g = 3 THEN g/(3-g) ELSE g END FROM generate_series(1, 5) AS g`
	for row := range cn.Query(t.Context(), q) {
		v, err := row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		if v != 1 {
			t.Fatalf("first row = %d, want 1", v)
		}
		break
	}

	// The drain encountered the error and recovered to ReadyForQuery.
	// The connection must be reusable.
	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec after break-drain error: %v", err)
	}
}

func TestQuerySequentialBreaks(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	ctx := t.Context()

	// Break out of three successive queries without consuming all rows.
	for _, q := range []string{
		"SELECT * FROM generate_series(1, 10)",
		"SELECT * FROM generate_series(20, 30)",
		"SELECT * FROM generate_series(100, 200)",
	} {
		for range cn.Query(ctx, q) {
			break
		}
	}

	// Fully consume a fourth query outside all prior ranges to prove
	// we're reading fresh results, not leftover rows from earlier queries.
	var got []int64
	for row := range cn.Query(ctx, "SELECT * FROM generate_series(1000, 1003)") {
		v, err := row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	diff.Test(t, t.Errorf, got, []int64{1000, 1001, 1002, 1003})
}

func TestQueryBreakCleanupPoisonsConn(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cn, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := cn.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, febe.ErrTerminated) {
			t.Errorf("Close() = %v", err)
		}
	})

	startCleanup := make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-startCleanup
		cn.cn.Close()
		close(done)
	}()

	for row := range cn.Query(ctx, "SELECT * FROM generate_series(1, 100000)") {
		_ = row
		close(startCleanup)
		<-done
		break
	}

	if err := cn.Exec(ctx, "SELECT 1"); err == nil {
		t.Fatal("expected Exec after cleanup failure to fail")
	}
}

func TestQueryEmptyQueryResponse(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	t.Cleanup(func() {
		checkReady(t, cn)
	})

	ctx := t.Context()

	var got []error
	for row := range cn.Query(ctx, "") {
		_, err := row.Column(0).Bytes()
		got = append(got, err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 error; got %d", len(got))
	}
	if !errors.Is(got[0], febe.ErrEmptyQuery) {
		t.Fatalf("expected ErrEmptyQuery; got %v", got[0])
	}
}

func TestQueryExtendedNumbers(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	const q = `
		SELECT
			encode(int2send($1::int2), 'hex'),
			encode(int2send($2::int2), 'hex'),
			encode(int4send($3::int4), 'hex'),
			encode(int4send($4::int4), 'hex'),
			encode(int8send($5::int8), 'hex'),
			encode(int8send($6::int8), 'hex'),
			encode(int8send($7::int8), 'hex'),
			encode(float4send($8::real), 'hex'),
			encode(float8send($9::double precision), 'hex')
	`

	args := []any{
		int8(-7),
		uint8(200),
		int32(-123456),
		uint16(60000),
		int64(1234567890123),
		int(-1234567890123),
		uint32(math.MaxUint32),
		float32(1.5),
		float64(-2.25),
	}

	var got [][]string
	for row := range cn.Query(t.Context(), q, args...) {
		got = append(got, collectValues(row))
	}

	want := [][]string{{
		"fff9",
		"00c8",
		"fffe1dc0",
		"0000ea60",
		"0000011f71fb04cb",
		"fffffee08e04fb35",
		"00000000ffffffff",
		"3fc00000",
		"c002000000000000",
	}}
	diff.Test(t, t.Errorf, got, want)
}

func TestEncodeArg(t *testing.T) {
	type customInt int64

	tests := []struct {
		name    string
		dst     []byte
		arg     any
		wantFmt int16
		want    []byte
		wantErr string
	}{
		{
			name:    "nil arg",
			dst:     []byte("unused"),
			arg:     nil,
			want:    nil,
			wantFmt: formatBinary,
		},
		{
			name:    "nil bytes arg",
			dst:     []byte("unused"),
			arg:     []byte(nil),
			want:    nil,
			wantFmt: formatBinary,
		},
		{
			name:    "byte slice arg",
			dst:     []byte("prefix:"),
			arg:     []byte("hello"),
			want:    []byte{'p', 'r', 'e', 'f', 'i', 'x', ':', 'h', 'e', 'l', 'l', 'o'},
			wantFmt: formatBinary,
		},
		{
			name:    "string arg",
			dst:     []byte("prefix:"),
			arg:     "hello",
			want:    []byte{'p', 'r', 'e', 'f', 'i', 'x', ':', 'h', 'e', 'l', 'l', 'o'},
			wantFmt: formatBinary,
		},
		{
			name:    "int8 arg",
			arg:     int8(-8),
			want:    []byte{0xff, 0xf8},
			wantFmt: formatBinary,
		},
		{
			name:    "uint8 arg",
			arg:     uint8(200),
			want:    []byte{0x00, 0xc8},
			wantFmt: formatBinary,
		},
		{
			name:    "int16 arg",
			arg:     int16(-123),
			want:    []byte{0xff, 0x85},
			wantFmt: formatBinary,
		},
		{
			name:    "uint16 arg",
			arg:     uint16(60000),
			want:    []byte{0x00, 0x00, 0xea, 0x60},
			wantFmt: formatBinary,
		},
		{
			name:    "int32 arg",
			arg:     int32(-123456789),
			want:    []byte{0xf8, 0xa4, 0x32, 0xeb},
			wantFmt: formatBinary,
		},
		{
			name:    "float32 arg",
			arg:     float32(1.5),
			want:    []byte{0x3f, 0xc0, 0x00, 0x00},
			wantFmt: formatBinary,
		},
		{
			name:    "int64 arg",
			arg:     int64(-7),
			want:    []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf9},
			wantFmt: formatBinary,
		},
		{
			name:    "float64 arg",
			arg:     float64(-2.25),
			want:    []byte{0xc0, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			wantFmt: formatBinary,
		},
		{
			name:    "time arg",
			arg:     time.Date(2024, 2, 3, 4, 5, 6, 987654321, time.FixedZone("UTC+2", 2*60*60)),
			want:    []byte{0x00, 0x02, 0xb3, 0x6f, 0x6d, 0x20, 0x2a, 0x86},
			wantFmt: formatBinary,
		},
		{
			name:    "time arg far-future beyond unixmicro limit",
			arg:     time.Date(294260, 1, 1, 0, 0, 0, 0, time.UTC),
			want:    []byte{0x7f, 0xfe, 0x17, 0x5f, 0xd1, 0x2b, 0xe0, 0x00},
			wantFmt: formatBinary,
		},
		{
			name:    "int arg",
			arg:     int(-123456),
			want:    []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0x1d, 0xc0},
			wantFmt: formatBinary,
		},
		{
			name:    "uint32 arg",
			arg:     uint32(math.MaxUint32),
			want:    []byte{0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff},
			wantFmt: formatBinary,
		},
		{
			name:    "uint arg",
			arg:     uint(42),
			want:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a},
			wantFmt: formatBinary,
		},
		{
			name:    "uint64 arg",
			arg:     uint64(42),
			want:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a},
			wantFmt: formatBinary,
		},
		{
			name:    "unsupported arg type bool",
			arg:     true,
			wantErr: "polite: unsupported argument type bool",
			wantFmt: formatBinary,
		},
		{
			name:    "unsupported alias int",
			arg:     customInt(7),
			wantErr: "polite: unsupported argument type",
			wantFmt: formatBinary,
		},
		{
			name:    "unsigned overflow wraps to int64",
			arg:     uint64(math.MaxUint64),
			want:    []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			wantFmt: formatBinary,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			got, gotFmt, err := appendArg(tt.dst, tt.arg)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("got error %v", err)
				}
				if !bytes.Equal(got, tt.want) {
					t.Fatalf("got=%v want=%v", got, tt.want)
				}
				if gotFmt != tt.wantFmt {
					t.Fatalf("format got=%v want=%v", gotFmt, tt.wantFmt)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got err %v want substring %q", err, tt.wantErr)
			}
			if tt.want != nil {
				t.Fatalf("expected no successful value; got %v", got)
			}
		})
	}
}

func TestQueryExtendedMixedArguments(t *testing.T) {
	t.Parallel()

	check := testutil.Checker(t)
	cn := newSession(t)

	in := time.Date(2026, 1, 2, 3, 4, 5, 987654321, time.FixedZone("UTC+3", 3*60*60))
	wantTime := wantTimestamptz(in)

	const q = `
		SELECT
			$1::text,
			$2::bytea,
			$3::timestamptz,
			$4::int8,
			$5::double precision
	`

	var gotText string
	var gotBytes []byte
	var gotTime time.Time
	var gotInt int64
	var gotFloat []byte
	for row := range cn.Query(t.Context(), q, "hello", []byte("world"), in, uint(42), float64(3.5)) {
		var err error
		gotText, err = row.Column(0).DecodeText()
		check(err, nil)
		gotBytes, err = row.Column(1).Bytes()
		check(err, nil)
		gotTime, err = row.Column(2).DecodeTime()
		check(err, nil)
		gotInt, err = row.Column(3).DecodeInt()
		check(err, nil)
		gotFloat, err = row.Column(4).Bytes()
		check(err, nil)
	}

	diff.Test(t, t.Errorf, gotText, "hello")
	diff.Test(t, t.Errorf, string(gotBytes), "world")
	diff.Test(t, t.Errorf, gotTime, wantTime)
	diff.Test(t, t.Errorf, gotInt, int64(42))
	diff.Test(t, t.Errorf, gotFloat, []byte{0x40, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
}

func TestExecLimit1(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	var got []byte
	ctx := febe.WithTrace(t.Context(), &febe.Trace{
		ReceivedHeader: func(ctx context.Context, h febe.MessageHeader) {
			got = append(got, h.Type())
		},
	})
	if err := cn.Exec(ctx, "SELECT generate_series(1, 3)"); err != nil {
		t.Fatal(err)
	}

	want := []byte("1tTZ2DDDCZ")
	diff.Test(t, t.Errorf, got, want)
}

func TestExec(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	if err := cn.Exec(t.Context(), "CREATE TEMP TABLE foo (bar INT)"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(t.Context(), "INSERT INTO foo VALUES (1)"); err != nil {
		t.Fatal(err)
	}
}

func TestExecExtended(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	if err := cn.Exec(t.Context(), "CREATE TEMP TABLE foo_any (name TEXT, payload BYTEA, n BIGINT)"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(t.Context(), "INSERT INTO foo_any VALUES ($1, $2, $3)", "hello", []byte("world"), uint32(7)); err != nil {
		t.Fatal(err)
	}

	got := collectRows(cn.Query(t.Context(), "SELECT name, payload, n FROM foo_any"))
	diff.Test(t, t.Errorf, got, [][]string{{"hello", "world", "\x00\x00\x00\x00\x00\x00\x00\a"}})
}

func TestErrors(t *testing.T) {
	ctx := t.Context()
	t.Run("prepare error", func(t *testing.T) {
		t.Parallel()
		cn := newSession(t)
		err := cn.Exec(ctx, "SELECT !")
		if err == nil {
			t.Fatal("expected error")
		}
		var fe *febe.Error
		if !errors.As(err, &fe) {
			t.Fatalf("got %T, want *febe.Error", err)
		}
		if !strings.Contains(fe.Message, "syntax error") {
			t.Fatalf("got %v, want syntax error", err)
		}

		// ensure we recover
		if err := cn.Exec(ctx, "SELECT 1"); err != nil {
			t.Fatalf("SELECT 1 failed: %v", err)
		}
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()
		cn := newSession(t)
		err := cn.Exec(ctx, "SELECT raise_error()")
		if err == nil {
			t.Fatal("expected error")
		}
		var fe *febe.Error
		if !errors.As(err, &fe) {
			t.Fatalf("got %T, want *febe.Error", err)
		}
		if !strings.Contains(fe.Message, "boom error") {
			t.Fatalf("got %v, want boom", err)
		}

		// ensure we recover
		if err := cn.Exec(ctx, "SELECT 1"); err != nil {
			t.Fatalf("SELECT 1 failed: %v", err)
		}
	})

	t.Run("terminal error is sticky", func(t *testing.T) {
		ctx := t.Context()
		cn, err := Dial(ctx, testDSN, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			err := cn.Close()
			if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, febe.ErrTerminated) {
				t.Fatal(err)
			}
		})

		terminal := cn.Exec(ctx, "SELECT pg_terminate_backend(pg_backend_pid())")
		if terminal == nil {
			t.Fatal("expected error")
		}

		if err := cn.Exec(ctx, "SELECT 1"); err == nil {
			t.Fatal("Exec(SELECT 1) = nil; want sticky terminal error")
		} else if !errors.Is(err, terminal) && !errors.Is(terminal, err) {
			t.Fatalf("Exec(SELECT 1) = %v; want sticky match for %v", err, terminal)
		}
	})
}

func TestQueryCacheHitAfterError(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	ctx := t.Context()

	// Run a parameterized query and verify the result.
	for row := range cn.Query(ctx, "SELECT $1::text", "hello") {
		got, err := row.Column(0).DecodeText()
		if err != nil {
			t.Fatal(err)
		}
		if got != "hello" {
			t.Fatalf("first query got %q, want %q", got, "hello")
		}
	}

	// Run a different query that errors and recovers.
	cn.Exec(ctx, "SELECT raise_error()")

	// Re-run the same parameterized query with a different value.
	// The cached statement from the first run must still work after
	// error recovery on an unrelated query.
	for row := range cn.Query(ctx, "SELECT $1::text", "world") {
		got, err := row.Column(0).DecodeText()
		if err != nil {
			t.Fatal(err)
		}
		if got != "world" {
			t.Fatalf("second query got %q, want %q", got, "world")
		}
	}
}

// TestStmtCacheKeyParamOIDs pins that the per-connection prepared-
// statement cache distinguishes the same SQL with different parameter
// OIDs: a Go int and a Go string against the same SELECT $1::text
// must produce two separate server-side prepared statements, not
// reuse one. pg_prepared_statements is the source of truth — both
// Parses land in it on this session.
func TestStmtCacheKeyParamOIDs(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	ctx := t.Context()
	const q = `SELECT $1::text`

	if err := cn.Exec(ctx, q, 42); err != nil { // pins int8
		t.Fatal(err)
	}
	if err := cn.Exec(ctx, q, "x"); err != nil { // pins text
		t.Fatal(err)
	}

	var n int64
	const meta = `SELECT count(*) FROM pg_prepared_statements WHERE statement = $1`
	for row := range cn.Query(ctx, meta, q) {
		n, _ = row.Column(0).DecodeInt()
	}
	if n != 2 {
		t.Fatalf("pg_prepared_statements count for %q = %d; want 2", q, n)
	}
}

// TestStmtCacheEvictsOnPlanChanged pins polite's recovery for the case
// where DDL invalidates a server-side prepared plan: PostgreSQL returns
// "cached plan must not change result type" (SQLSTATE 0A000), polite
// drops the matching cache entry, and the next call to the same SQL
// re-Parses cleanly. The connection is left usable; only the cached
// statement is evicted.
func TestStmtCacheEvictsOnPlanChanged(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	ctx := t.Context()

	if err := cn.Exec(ctx, "CREATE TEMP TABLE poison (n text)"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(ctx, "INSERT INTO poison VALUES ('hello')"); err != nil {
		t.Fatal(err)
	}

	// Cache miss: prepare and cache the statement on this connection.
	if err := cn.Exec(ctx, "SELECT n FROM poison"); err != nil {
		t.Fatal(err)
	}

	// Mutate the column type underneath the cached prepared statement.
	if err := cn.Exec(ctx, "ALTER TABLE poison ALTER COLUMN n TYPE int4 USING 0"); err != nil {
		t.Fatal(err)
	}

	// Cache hit against an invalidated plan: PG raises 0A000.
	err := cn.Exec(ctx, "SELECT n FROM poison")
	if !errors.Is(err, febe.ErrorCode("0A000")) {
		t.Fatalf("first execute after ALTER: err = %v; want SQLSTATE 0A000", err)
	}

	// The connection is still usable.
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Errorf("SELECT 1 after 0A000 = %v; want nil (connection should survive)", err)
	}

	// Cache entry was evicted, so the same SQL re-Parses cleanly against
	// the post-ALTER schema.
	if err := cn.Exec(ctx, "SELECT n FROM poison"); err != nil {
		t.Errorf("re-execute after 0A000 = %v; want nil (cache should have been evicted)", err)
	}

	// Two prepared statements remain for this SQL: the zombie from
	// before the ALTER, and the fresh re-Parse. polite evicts only
	// the client-side cache entry; the server side lingers until the
	// connection closes. Intentional, not promised. Cycle long-lived
	// connections, or grow Pool to do it.
	var n int64
	for row := range cn.Query(ctx, `SELECT count(*) FROM pg_prepared_statements WHERE statement = $1`, "SELECT n FROM poison") {
		n, _ = row.Column(0).DecodeInt()
	}
	if n != 2 {
		t.Errorf("pg_prepared_statements count = %d; want 2 (zombie + re-Parse)", n)
	}
}

// TestQueryIteratorSingleUse pins that the iter.Seq returned by
// Query is single-use: try 0 (the first invocation of the seq)
// succeeds; tries 1-3 (reuses) each yield a Row that reports an
// error. Calling Query again is how a caller re-executes the SQL.
func TestQueryIteratorSingleUse(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	ctx := t.Context()

	seq := cn.Query(ctx, "SELECT 1")
	for try := range 4 {
		next, stop := iter.Pull(seq)
		row, ok := next()
		stop()
		if !ok {
			t.Fatalf("try %d: next returned ok=false; want a row", try)
		}
		_, err := row.Column(0).Bytes()
		switch {
		case try == 0 && err != nil:
			t.Errorf("try 0: err = %v; want nil", err)
		case try > 0 && err == nil:
			t.Errorf("try %d: err = nil; want non-nil (iterator already used)", try)
		}
	}
}

func TestNotice(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	notice := func(ctx context.Context) {
		t.Helper()
		if err := cn.Exec(ctx, "SELECT raise_notice()"); err != nil {
			t.Fatal(err)
		}
	}

	ctx := t.Context()

	// no handler
	notice(ctx)
	checkReady(t, cn) // check notice is dropped

	var got []*febe.Notice
	ctx = febe.WithTrace(ctx, &febe.Trace{
		ReceivedNotice: func(ctx context.Context, notice *febe.Notice) {
			got = append(got, notice)
		},
	})

	notice(ctx)
	want := []*febe.Notice{
		{
			Severity:  "NOTICE",
			Verbosity: "NOTICE",
			Message:   "boom notice",
		},
	}
	keep := diff.KeepFields[febe.Notice]("Severity", "Verbosity", "Message")
	diff.Test(t, t.Errorf, got, want, keep)
}

func TestParameterStatus(t *testing.T) {
	t.Parallel()

	cn := newSessionDB(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	var got []febe.Parameter
	ctx := febe.WithTrace(t.Context(), &febe.Trace{
		ReceivedParameterStatus: func(ctx context.Context, p febe.Parameter) {
			got = append(got, p)
		},
	})

	if err := cn.Exec(ctx, "SET datestyle TO postgres, dmy"); err != nil {
		t.Fatal(err)
	}

	checkReady(t, cn) // flush out parameter status if not received by Exec

	diff.Test(t, t.Errorf, got, []febe.Parameter{
		{Name: "DateStyle", Value: "Postgres, DMY"},
	})
}

func TestParameterStatusTraceCompatibility(t *testing.T) {
	t.Parallel()

	cn := newSessionDB(t)
	defer checkReady(t, cn)

	var got int
	ctx := febe.WithTrace(t.Context(), &febe.Trace{
		ReceivedParameterStatus: func(ctx context.Context, p febe.Parameter) {
			got++
		},
	})

	if err := cn.Exec(ctx, "SET datestyle TO postgres, dmy"); err != nil {
		t.Fatal(err)
	}
	checkReady(t, cn)
	if got == 0 {
		t.Fatal("expected ReceivedParameterStatus callback to fire")
	}
}

func TestStatus(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	if got := cn.fc.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("initial Status() = %q; want %q", got, febe.ReadyStatusIdle)
	}

	if err := cn.Exec(t.Context(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if got := cn.fc.Status(); got != febe.ReadyStatusBusy {
		t.Fatalf("after BEGIN Status() = %q; want %q", got, febe.ReadyStatusBusy)
	}

	if err := cn.Exec(t.Context(), "SELECT 1/0"); err == nil {
		t.Fatal("expected error from SELECT 1/0")
	}
	if got := cn.fc.Status(); got != febe.ReadyStatusFailed {
		t.Fatalf("after failed statement Status() = %q; want %q", got, febe.ReadyStatusFailed)
	}

	if err := cn.Exec(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if got := cn.fc.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("after ROLLBACK Status() = %q; want %q", got, febe.ReadyStatusIdle)
	}
}

func TestByteaString(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	ctx := t.Context()
	var got []string
	for row := range cn.Query(ctx, "SELECT '\x12\x345'::bytea") {
		got = append(got, collectValues(row)...)
	}
	want := []string{"\x12\x345"}
	diff.Test(t, t.Errorf, got, want)
}

func TestRowDecodeFloat(t *testing.T) {
	t.Parallel()

	check := testutil.Checker(t)
	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	ctx := t.Context()
	var got []float64
	for row := range cn.Query(ctx, `SELECT 1.5::real, -2.25::double precision`) {
		for col := range row.Columns() {
			f, err := col.DecodeFloat()
			check(err, nil)
			got = append(got, f)
		}
	}
	want := []float64{1.5, -2.25}
	diff.Test(t, t.Errorf, got, want)
}

func TestRowDecodeBool(t *testing.T) {
	t.Parallel()

	check := testutil.Checker(t)
	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	ctx := t.Context()
	var got []bool
	for row := range cn.Query(ctx, `SELECT true, false`) {
		for col := range row.Columns() {
			b, err := col.DecodeBool()
			check(err, nil)
			got = append(got, b)
		}
	}
	want := []bool{true, false}
	diff.Test(t, t.Errorf, got, want)
}

// TestDecodeBoolWrongWidth pins that DecodeBool rejects non-bool widths
// rather than synthesizing a boolean from a wider value. This is the
// design choice that keeps text 'false' (5 non-zero bytes) and friends
// from masquerading as true.
func TestDecodeBoolWrongWidth(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	cases := []struct {
		name string
		sql  string
	}{
		{"int2", `SELECT 1::int2`},
		{"int4", `SELECT 1::int4`},
		{"int8", `SELECT 1::int8`},
		{"text_false", `SELECT 'false'::text`},
		{"empty_text", `SELECT ''::text`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for row := range cn.Query(t.Context(), tc.sql) {
				_, err := row.Column(0).DecodeBool()
				if err == nil {
					t.Fatal("DecodeBool err = nil; want invalid length")
				}
				if !strings.Contains(err.Error(), "invalid length") {
					t.Fatalf("DecodeBool err = %q; want substring %q", err, "invalid length")
				}
				return
			}
			t.Fatal("expected 1 row")
		})
	}
}

func TestColumnType(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	ctx := t.Context()
	var got []struct {
		Name string
		Size int
		Type oids.ID
	}
	for row := range cn.Query(ctx, `SELECT 'hello'::text AS text, 42::int4 AS int4, now()::timestamptz AS timestamptz`) {
		for col := range row.Columns() {
			got = append(got, struct {
				Name string
				Size int
				Type oids.ID
			}{
				Name: col.Name(),
				Size: col.Size(),
				Type: col.Type(),
			})
		}
	}

	want := []struct {
		Name string
		Size int
		Type oids.ID
	}{
		{Name: "text", Size: -1, Type: oids.Text},
		{Name: "int4", Size: 4, Type: oids.Int4},
		{Name: "timestamptz", Size: 8, Type: oids.Timestamptz},
	}
	diff.Test(t, t.Errorf, got, want)
}

func checkReady(t *testing.T, c *Conn) {
	t.Helper()
	if t.Failed() {
		return
	}
	if got := c.fc.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("Status() = %q; want %q", got, febe.ReadyStatusIdle)
	}
	if err := c.Exec(t.Context(), "SELECT 1"); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		t.Fatal(err)
	}
}

func TestPoolPutDiscardsTransaction(t *testing.T) {
	t.Parallel()

	p := newTestPool(t)
	p.MaxItems = 1

	ctx := t.Context()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	p.Put(cn) // not idle — discarded and retired

	cn, err = p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put(cn)

	if !cn.fc.Status().Idle() {
		t.Fatalf("Status() = %v; want idle", cn.fc.Status())
	}
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestPoolPutDiscardsFailedTransaction(t *testing.T) {
	t.Parallel()

	p := newTestPool(t)
	p.MaxItems = 1

	ctx := t.Context()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	cn.Exec(ctx, "SELECT * FROM nonexistent_table_xyz")
	if cn.fc.Status() != febe.ReadyStatusFailed {
		t.Fatalf("Status() = %v; want failed", cn.fc.Status())
	}
	p.Put(cn) // not idle — discarded and retired

	cn, err = p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put(cn)

	if !cn.fc.Status().Idle() {
		t.Fatalf("Status() = %v; want idle", cn.fc.Status())
	}
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestPoolPutIdle(t *testing.T) {
	t.Parallel()

	p := newTestPool(t)
	p.MaxItems = 1

	ctx := t.Context()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !cn.fc.Status().Idle() {
		t.Fatal("expected idle after Get")
	}
	p.Put(cn)

	cn, err = p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put(cn)

	if !cn.fc.Status().Idle() {
		t.Fatalf("Status() = %v; want idle", cn.fc.Status())
	}
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestPoolPutPoisoned(t *testing.T) {
	t.Parallel()

	p := newTestPool(t)
	p.MaxItems = 1

	ctx := t.Context()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Poison the connection by closing the underlying net.Conn.
	cn.cn.Close()
	cn.Exec(ctx, "SELECT 1") // force the error to latch
	p.Put(cn)

	// The pool should discard the poisoned connection and open a
	// fresh one. If the pool leaks the item slot, this Get will
	// hang because MaxItems=1 and no load will be triggered.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	cn, err = p.Get(ctx)
	if err != nil {
		t.Fatalf("Get after poisoned Put: %v", err)
	}
	defer p.Put(cn)

	if !cn.fc.Status().Idle() {
		t.Fatalf("Status() = %v; want idle", cn.fc.Status())
	}
	if err := cn.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestPoolPutDiscardsCopyFrom(t *testing.T) {
	t.Parallel()

	p := newTestPool(t)
	p.MaxItems = 1

	ctx := t.Context()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = cn.Exec(ctx, "CREATE TEMP TABLE discard_copy (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	// Enter COPY FROM mode and return the connection to the pool
	// without finishing the COPY. The pool should discard it.
	for row := range cn.Query(ctx, "COPY discard_copy FROM STDIN WITH (FORMAT binary)") {
		_, err = row.Column(0).Bytes()
		if !errors.Is(err, ErrCopyFrom) {
			t.Fatalf("column error = %v, want ErrCopyFrom", err)
		}
		p.Put(cn)
		break
	}

	// The pool should discard the mid-COPY connection and open a
	// fresh one. If the pool leaks the item slot, this Get will
	// hang because MaxItems=1 and no load will be triggered.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	cn, err = p.Get(ctx)
	if err != nil {
		t.Fatalf("Get after mid-COPY Put: %v", err)
	}
	defer p.Put(cn)

	if !cn.fc.Status().Idle() {
		t.Fatalf("Status() = %v; want idle", cn.fc.Status())
	}
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestEvents(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	listener, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	cn := newSession(t)

	if err := listener.Exec(ctx, "LISTEN test_events"); err != nil {
		t.Fatal(err)
	}

	// Send a notification from the other connection. NOTIFY is non-blocking
	// server-side; the message queues on the listener's connection until the
	// range loop below reads it.
	if err := cn.Exec(ctx, "NOTIFY test_events, 'hello'"); err != nil {
		t.Fatal(err)
	}

	for n := range Notifications(ctx, listener) {
		if got := string(n.Channel); got != "test_events" {
			t.Fatalf("Channel = %q; want %q", got, "test_events")
		}
		if got := string(n.Payload); got != "hello" {
			t.Fatalf("Payload = %q; want %q", got, "hello")
		}
		break
	}

	// Connection should still be usable after breaking out of Notifications.
	if err := listener.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

// TestNotificationsPreservesCallerTrace verifies that Notifications does
// not silently overwrite a caller-installed febe.Trace: the caller's
// ReceivedNotification must still fire, alongside polite's own yield.
func TestNotificationsPreservesCallerTrace(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	listener, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	cn := newSession(t)

	if err := listener.Exec(ctx, "LISTEN trace_events"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(ctx, "NOTIFY trace_events, 'hi'"); err != nil {
		t.Fatal(err)
	}

	var got []string
	ctx = febe.WithTrace(ctx, &febe.Trace{
		ReceivedNotification: func(_ context.Context, n *febe.Notification) {
			got = append(got, string(n.Payload))
		},
	})

	var yielded []string
	for n := range Notifications(ctx, listener) {
		yielded = append(yielded, string(n.Payload))
		break
	}

	if want := []string{"hi"}; !slices.Equal(yielded, want) {
		t.Fatalf("yielded = %q; want %q", yielded, want)
	}
	if want := []string{"hi"}; !slices.Equal(got, want) {
		t.Fatalf("caller trace got = %q; want %q", got, want)
	}
}

// TestNotificationsInsideQueryYieldsNothing verifies that calling
// Notifications from inside an active Conn.Query range body observes
// the busy guard and returns immediately without yielding.
func TestNotificationsInsideQueryYieldsNothing(t *testing.T) {
	listener := newSession(t)
	notifier := newSession(t)

	listener.Exec(t.Context(), "LISTEN test_busy_q_in_n")
	notifier.Exec(t.Context(), "NOTIFY test_busy_q_in_n, 'ping'")

	for row := range listener.Query(t.Context(), "SELECT 1") {
		if _, err := row.Column(0).Bytes(); err != nil {
			t.Fatal(err)
		}
		for range Notifications(t.Context(), listener) {
			t.Error("unexpected notification yielded inside Query")
		}
		return // success
	}

	t.Fatal("expected to receive row")
}

// TestQueryInsideNotificationsErrBusy verifies that calling Conn.Query
// from inside an active Notifications range body returns ErrBusy via
// the standard Row(0).Bytes() error path, leaving the wire untouched.
func TestQueryInsideNotificationsErrBusy(t *testing.T) {
	listener := newSession(t)
	notifier := newSession(t)

	listener.Exec(t.Context(), "LISTEN test_busy_n_in_q")
	notifier.Exec(t.Context(), "NOTIFY test_busy_n_in_q, 'ping'")

	for range Notifications(t.Context(), listener) {
		for row := range listener.Query(t.Context(), "SELECT 1") {
			_, err := row.Column(0).Bytes()
			if !errors.Is(err, ErrBusy) {
				t.Errorf("Query inside Notifications: err = %v; want ErrBusy", err)
			}
		}
		return // success
	}

	t.Fatal("expected to receive notification")
}

// TestQuerySQLCastPinsParamType verifies that polite pins each argument's
// PostgreSQL OID in the Parse message, so the backend casts on its side
// when the SQL wants a different type. Cases that previously needed a
// double cast (e.g. $1::text::int4 to bridge a Go int) now compose
// directly.
func TestQuerySQLCastPinsParamType(t *testing.T) {
	cn := newSession(t)

	t.Run("Go int pinned to int4", func(t *testing.T) {
		const q = `SELECT $1::int4`

		for row := range cn.Query(t.Context(), q, 42) {
			col := row.Column(0)
			if col.Type() != oids.Int4 {
				t.Errorf("column type = %s; want int4", col.Type())
			}
			if n, _ := col.DecodeInt(); n != 42 {
				t.Errorf("got %d; want 42", n)
			}

			return
		}

		t.Fatalf("expected one row")
	})

	t.Run("Go string pinned to inet", func(t *testing.T) {
		const q = `SELECT host($1::inet)`

		for row := range cn.Query(t.Context(), q, "192.168.1.42") {
			if s, _ := row.Column(0).DecodeText(); s != "192.168.1.42" {
				t.Errorf("got %q; want %q", s, "192.168.1.42")
			}

			return
		}

		t.Fatalf("expected one row")
	})

	t.Run("time.Time pinned to timestamp via $1::timestamp", func(t *testing.T) {
		// Polite sends timestamptz; PostgreSQL casts to timestamp on
		// its side because the SQL says ::timestamp.
		want := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
		const q = `SELECT $1::timestamp`

		for row := range cn.Query(t.Context(), q, want) {
			col := row.Column(0)
			if col.Type() != oids.Timestamp {
				t.Errorf("column type = %s; want timestamp", col.Type())
			}
			if got, _ := col.DecodeTime(); !got.Equal(want) {
				t.Errorf("got %v; want %v", got, want)
			}

			return
		}

		t.Fatalf("expected one row")
	})
}

var testDSN = cmp.Or(
	os.Getenv("DATABASE_URL"),
	"dbname=postgres host=/tmp port=5411 sslmode=disable user=postgres password=postgres",
)
