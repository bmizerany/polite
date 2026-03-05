package pqlite

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"iter"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"blake.io/pqlite/febe"
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

	cn, err := Open(t.Context(), testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}

	tname := strings.ReplaceAll(t.Name(), "/", "_")
	dbname := fmt.Sprintf("gotest_%s_%s", tname, rand.Text()[:6])
	dbname = strings.ToLower(dbname)
	if err := cn.Exec(t.Context(), "CREATE DATABASE "+dbname); err != nil {
		t.Fatal(err)
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

	cn, err := Open(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cn.Close(); err != nil {
			t.Fatal(err)
		}
	})
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	return cn
}

var funcsCreated atomic.Bool

func newSession(t testing.TB) *Conn {
	ctx := t.Context()
	cn, err := Open(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cn.Close(); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			t.Fatal(err)
		}
	})

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

func TestQuery(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	for range 3 {
		const q = `SELECT 1, 2, 'three'`
		var got [][]any
		for row, err := range cn.Query(t.Context(), q) {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, []any{
				must(row.ParseInt()),
				must(row.ParseInt()),
				must(row.ParseString()),
			})
		}
		want := [][]any{{1, 2, "three"}}
		diff.Test(t, t.Errorf, got, want)
	}
}

func TestQueryAllocations(t *testing.T) {
	// NOTE: We cannot use t.Parallel() because it seems to interfere with
	// AllocsPerRun.

	cn := newSession(t)
	allocs := testing.AllocsPerRun(1000, func() {
		const q = `SELECT 1, 'two'`
		for row, err := range cn.Query(t.Context(), q) {
			if err != nil {
				t.Fatal(err)
			}
			_, err := row.ParseInt()
			if err != nil {
				t.Fatal(err)
			}

			// We use View because ParseString will allocate by
			// allocate by design.
			_, err = row.View()
			if err != nil {
				t.Fatal(err)
			}
		}
	})
	if allocs > 0 {
		t.Errorf("expected 0 allocations; got %v", allocs)
	}
}

func TestDeadlineIsolation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c, err := Open(ctx, testDSN, nil)
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
	if err := cn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cn.Close(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Close() = %v; want %v", err, ErrPoolClosed)
	}

	// Ensure we can't use the connection after closing.
	if err := cn.Exec(t.Context(), "SELECT 1"); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("query(SELECT 1) = %v; want %v", err, ErrPoolClosed)
	}
}

func TestRowClose(t *testing.T) {
	t.Parallel()

	const q = `SELECT 1`

	cn := newSession(t)

	t.Run("with remaining values", func(t *testing.T) {
		for row, err := range cn.Query(t.Context(), q) {
			if err != nil {
				t.Fatal(err)
			}
			if err := row.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("without remaining values", func(t *testing.T) {
		for row, err := range cn.Query(t.Context(), q) {
			if err != nil {
				t.Fatal(err)
			}
			_, err := row.View()
			if err != nil {
				t.Fatal(err)
			}
			if err := row.Close(); err != nil {
				t.Fatal(err)
			}
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
	for row, err := range cn.Query(t.Context(), q, "hello", "world") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, must(row.ParseString()))
	}

	want := []string{"hello", "world"}
	diff.Test(t, t.Errorf, got, want)
}

func collectRows(t testing.TB, rows iter.Seq2[*Row, error]) [][]string {
	var got [][]string
	for row, err := range rows {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, collectValues(row))
	}
	return got
}

func collectValues(row *Row) []string {
	var got []string
	for range row.Columns() {
		got = append(got, must(row.ParseString()))
	}
	return got
}

func TestQueryAll(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	var got [][]string
	for row, err := range cn.Query(t.Context(), "SELECT * FROM generate_series(1, 5)") {
		if err != nil {
			t.Fatal(err)
		}
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
	var got [][]string
	for row, err := range cn.Query(t.Context(), "SELECT * FROM generate_series(1, 5)") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, collectValues(row))
		break
	}
	want := [][]string{
		{"\x00\x00\x00\x01"},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryUnprocceedColumnsPanic(t *testing.T) {
	t.Skip(`
		Currently just prints the error. Panicing is also probably not the best
		behavior. This should test the conn shoudl recover if possible, otherwise mark
		the conn as bad and prevent future use.
	`)

	t.Parallel()

	defer func() {
		r := recover()
		if r == nil || !strings.Contains(fmt.Sprint(r), "unprocessed columns") {
			t.Error("expected panic about unprocessed columns; got:", r)
		}
	}()

	cn := newSession(t)
	var n int
	for row, err := range cn.Query(t.Context(), "SELECT generate_series(1, 3)") {
		if err != nil {
			t.Fatal(err)
		}
		if n++; n == 1 {
			break
		}
		if err := row.Close(); err != nil {
			t.Fatal(err)
		}
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
	for _, err := range cn.Query(ctx, "") {
		got = append(got, err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 error; got %d", len(got))
	}
	if !errors.Is(got[0], febe.ErrEmptyQuery) {
		t.Fatalf("expected ErrEmptyQuery; got %v", got[0])
	}
}

func TestExecSelectTag(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	want := Result{
		RowsAffected: 1,
	}
	diff.Test(t, t.Errorf, cn.LastResult(), want)
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
	want := Result{
		RowsAffected: 1,
	}
	diff.Test(t, t.Errorf, cn.LastResult(), want)
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

func TestByteaString(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	ctx := t.Context()
	var got []string
	for row, err := range cn.Query(ctx, "SELECT '\x12\x345'::bytea") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, must(row.ParseString()))
	}
	want := []string{"\x12\x345"}
	diff.Test(t, t.Errorf, got, want)
}

func TestRowParseTime(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	ctx := t.Context()
	var got []time.Time
	for row, err := range cn.Query(ctx, `SELECT '2021-01-01 00:00:00'::timestamp`) {
		if err != nil {
			t.Fatal(err)
		}
		ts, err := row.ParseTime()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ts)
	}
	want := []time.Time{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)}
	diff.Test(t, t.Errorf, got, want)
}

func TestUseGoTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cn, err := Open(t.Context(), testDSN, &Config{UseGoTime: true})
		if err != nil {
			t.Fatal(err)
		}
		defer cn.Close()

		check := func() {
			t.Helper()

			err := cn.Exec(t.Context(), "CREATE TEMP TABLE IF NOT EXISTS synctest (t TIMESTAMPTZ DEFAULT now())")
			if err != nil {
				t.Fatal(err)
			}

			const q = `
				WITH ins AS (
					INSERT INTO synctest DEFAULT VALUES
					RETURNING t
				)

				-- Select all variations of now()
				-- and check the equal the synctest bubble time.
				SELECT now(), transaction_timestamp(), statement_timestamp(), t FROM ins
			`

			for row, err := range cn.Query(t.Context(), q) {
				if err != nil {
					t.Fatal(err)
				}
				if row.Columns() <= 0 {
					t.Fatal("expected at least one column")
				}
				for i := range row.Columns() {
					v, _ := row.ParseTime()
					if !v.Equal(time.Now().Truncate(time.Microsecond)) {
						t.Fatalf("[%d]: got %v; want %v", i, v, time.Now())
					}
				}

				return
			}

			t.Fatal("expected at least one row")
		}

		check()
		time.Sleep(1 * time.Hour)
		check()
	})
}

func checkReady(t *testing.T, c *Conn) {
	t.Helper()
	if t.Failed() {
		return
	}
	if err := c.Exec(t.Context(), "SELECT 1"); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		t.Fatal(err)
	}
}

var testDSN = cmp.Or(os.Getenv("DATABASE_URL"),
	"dbname=postgres host=/tmp port=5411 sslmode=disable user=postgres password=postgres")
