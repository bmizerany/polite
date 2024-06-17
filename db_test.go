package pqlite

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync/atomic"
	"testing"
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

func createDB(t testing.TB) string {
	t.Helper()

	cn, err := Open(context.Background(), mainDSN, nil)
	if err != nil {
		t.Fatal(err)
	}

	tname := strings.Replace(t.Name(), "/", "_", -1)
	dbname := fmt.Sprintf("test_%s_%s", tname, randomString())
	dbname = strings.ToLower(dbname)
	if err := cn.Exec(context.Background(), "CREATE DATABASE "+dbname); err != nil {
		t.Fatal(err)
	}
	dsn := mainDSN + " dbname=" + dbname
	t.Logf("newConn: dsn: %q", dsn)
	return dsn
}

func newSessionDB(t testing.TB) *Conn {
	ctx := context.Background()
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
	ctx := context.Background()
	cn, err := Open(ctx, mainDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cn.Close(); err != nil {
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
		if err := cn.Exec(context.Background(), sql); err != nil {
			t.Fatal(err)
		}
	}
	return cn
}

func TestQuery(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	const q = `SELECT 1, 2, 'three'`
	var got [][]any
	for row, err := range cn.Query(context.Background(), q) {
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

func TestQueryAllocations(t *testing.T) {
	// If t.Parallel is set, this test will fail.

	cn := newSession(t)

	allocs := testing.AllocsPerRun(1000, func() {
		const q = `SELECT 1, 'two'`
		for row, err := range cn.Query(context.Background(), q) {
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

func TestRowClose(t *testing.T) {
	t.Parallel()

	const q = `SELECT 1`

	cn := newSession(t)

	t.Run("with remaining values", func(t *testing.T) {
		for row, err := range cn.Query(context.Background(), q) {
			if err != nil {
				t.Fatal(err)
			}
			if err := row.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("without remaining values", func(t *testing.T) {
		for row, err := range cn.Query(context.Background(), q) {
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
	for row, err := range cn.Query(context.Background(), q, "hello", "world") {
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
	for row, err := range cn.Query(context.Background(), "SELECT * FROM generate_series(1, 5)") {
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
	for row, err := range cn.Query(context.Background(), "SELECT * FROM generate_series(1, 5)") {
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
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil || !strings.Contains(fmt.Sprint(r), "unprocessed columns") {
			t.Error("expected panic about unprocessed columns; got:", r)
		}
	}()

	cn := newSession(t)
	var n int
	for row, err := range cn.Query(context.Background(), "SELECT generate_series(1, 3)") {
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
		checkPing(t, cn)
	})

	ctx := context.Background()

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
	if err := cn.Exec(context.Background(), "SELECT 1"); err != nil {
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
	ctx := febe.WithTrace(context.Background(), &febe.Trace{
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
	if err := cn.Exec(context.Background(), "CREATE TEMP TABLE foo (bar INT)"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(context.Background(), "INSERT INTO foo VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	want := Result{
		RowsAffected: 1,
	}
	diff.Test(t, t.Errorf, cn.LastResult(), want)
}

func TestErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("prepare error", func(t *testing.T) {
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
		if err := cn.Ping(ctx); err != nil {
			t.Fatalf("ping failed: %v", err)
		}
	})

	t.Run("exec error", func(t *testing.T) {
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
		if err := cn.Ping(ctx); err != nil {
			t.Fatalf("ping failed: %v", err)
		}
	})
}

func TestNotice(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkPing(t, cn) // ensure we leave in a good state

	notice := func(ctx context.Context) {
		t.Helper()
		if err := cn.Exec(ctx, "SELECT raise_notice()"); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()

	// no handler
	notice(ctx)
	checkPing(t, cn) // check notice is dropped

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
	defer checkPing(t, cn) // ensure we leave in a good state

	var got []febe.Parameter
	ctx := febe.WithTrace(context.Background(), &febe.Trace{
		ReceivedParameterStatus: func(ctx context.Context, p febe.Parameter) {
			got = append(got, p)
		},
	})

	if err := cn.Exec(ctx, "SET datestyle TO postgres, dmy"); err != nil {
		t.Fatal(err)
	}

	checkPing(t, cn) // flush out parameter status if not received by Exec

	diff.Test(t, t.Errorf, got, []febe.Parameter{
		{Name: "DateStyle", Value: "Postgres, DMY"},
	})
}

func TestByteaString(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkPing(t, cn) // ensure we leave in a good state

	ctx := context.Background()
	if err := cn.Exec(ctx, "CREATE TEMP TABLE foo (bar BYTEA)"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(ctx, `INSERT INTO foo VALUES ($1)`, "\x12\x345"); err != nil {
		t.Fatal(err)
	}

	var got []string
	for row, err := range cn.Query(ctx, "SELECT bar FROM foo") {
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
	defer checkPing(t, cn) // ensure we leave in a good state

	ctx := context.Background()
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

func checkPing(t *testing.T, c *Conn) {
	t.Helper()
	if t.Failed() {
		return
	}
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

var (
	mainDSN = "dbname=postgres port=5433 host=localhost sslmode=disable user=postgres password=postgres"
)

// randomString uses math/rand to generate a random string of 8 printable
// characters.
func randomString() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = chars[b[i]%byte(len(chars))]
	}
	return string(b)
}
