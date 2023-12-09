package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os/user"
	"strings"
	"testing"

	"blake.io/postgres/febe"
	"github.com/jackc/pgx/v5"
	"kr.dev/diff"
)

func createDB(t testing.TB) string {
	tname := strings.Replace(t.Name(), "/", "_", -1)
	dbname := fmt.Sprintf("test_%s_%s", tname, randomString())
	dbname = strings.ToLower(dbname)
	_, err := mc.Exec(context.Background(), "CREATE DATABASE "+dbname)
	if err != nil {
		t.Fatal(err)
	}
	dsn := mainDSN + " dbname=" + dbname
	t.Logf("newConn: dsn: %q", dsn)
	return dsn
}

func newConn(t testing.TB) *Conn {
	ctx := context.Background()
	dsn := createDB(t)
	c, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	})
	_, err = c.Exec(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestQuery(t *testing.T) {
	c := newConn(t)
	rs := c.Query(context.Background(), "SELECT 1, 2, 'three'")
	var got [][][]byte
	for row, err := range rs.All {
		if err != nil {
			t.Fatal(err)
		}
		var coldata [][]byte
		for {
			data, err := row.Bytes()
			if err != nil {
				if errors.Is(err, ErrOutOfColumns) {
					break
				}
				t.Fatal(err)
			}
			t.Logf("data: %q", string(data))
			coldata = append(coldata, data)
		}
		got = append(got, coldata)
	}
	want := [][][]byte{
		{
			{'1'},
			{'2'},
			{'t', 'h', 'r', 'e', 'e'},
		},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryExtended(t *testing.T) {
	c := newConn(t)
	const q = `
		SELECT $1
		UNION ALL
		SELECT $2
	`
	var got []string
	for row, err := range c.QueryAll(context.Background(), q, "hello", "world") {
		if err != nil {
			t.Fatal(err)
		}
		data, err := row.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(data))
	}
	want := []string{"hello", "world"}
	diff.Test(t, t.Errorf, got, want)
}

func values(t testing.TB, row *Row) [][]byte {
	var got [][]byte
	for range row.Columns() {
		data, err := row.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, data)
	}
	return got
}

func TestQueryAll(t *testing.T) {
	c := newConn(t)
	var got [][][]byte
	for row, err := range c.QueryAll(context.Background(), "SELECT * FROM generate_series(1, 5)") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, values(t, row))
	}
	want := [][][]byte{
		{{'1'}},
		{{'2'}},
		{{'3'}},
		{{'4'}},
		{{'5'}},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryBreakInAll(t *testing.T) {
	c := newConn(t)
	var got [][][]byte
	for row, err := range c.QueryAll(context.Background(), "SELECT * FROM generate_series(1, 5)") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, values(t, row))
		break
	}
	want := [][][]byte{
		{{'1'}},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryUnprocceedColumnsPanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil || !strings.Contains(fmt.Sprint(r), "unprocessed columns") {
			t.Error("expected panic about unprocessed columns; got:", r)
		}
	}()

	c := newConn(t)
	var n int
	rs := c.Query(context.Background(), "SELECT generate_series(1, 3)")
	defer rs.Close()
	for row, err := range rs.All {
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
	t.Run("All", func(t *testing.T) {
		c := newConn(t)
		defer checkPing(t, c)

		rs := c.Query(context.Background(), "")
		defer rs.Close()

		var got []error
		for _, err := range rs.All {
			got = append(got, err)
		}
		if len(got) != 1 {
			t.Fatalf("expected 1 error; got %d", len(got))
		}
		if !errors.Is(got[0], ErrEmptyQuery) {
			t.Fatalf("expected ErrEmptyQuery; got %v", got[0])
		}
	})

	t.Run("Close", func(t *testing.T) {
		c := newConn(t)
		defer checkPing(t, c)
		got := c.Query(context.Background(), "").Close()
		if !errors.Is(got, ErrEmptyQuery) {
			t.Fatalf("got %v; want ErrEmptyQuery", got)
		}
	})
}

func TestExecSelectTag(t *testing.T) {
	c := newConn(t)
	got, err := c.Exec(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	want := Result{
		RowsAffected: 1,
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestExecLimit1(t *testing.T) {
	c := newConn(t)
	var got []byte
	ctx := febe.WithTrace(context.Background(), &febe.Trace{
		ReceivedHeader: func(ctx context.Context, h febe.MessageHeader) {
			const dataRow = 'T'
			if h.Type() == dataRow {
				got = append(got, h.Type())
			}
		},
	})
	_, err := c.Exec(ctx, "SELECT generate_series(1, 3)")
	if err != nil {
		t.Fatal(err)
	}

	want := []byte{'T'} // the first of the three data rows
	diff.Test(t, t.Errorf, got, want)
}

func TestExec(t *testing.T) {
	cn := newConn(t)
	_, err := cn.Exec(context.Background(), "CREATE TEMP TABLE foo (bar INT)")
	if err != nil {
		t.Fatal(err)
	}
	got, err := cn.Exec(context.Background(), "INSERT INTO foo VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	want := Result{
		RowsAffected: 1,
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestError(t *testing.T) {
	c := newConn(t)
	_, err := c.Exec(context.Background(), "SELECT * FROM nothing")
	if err == nil {
		t.Fatal("expected error")
	}
	t.Log(err)
}

func TestNotice(t *testing.T) {
	c := newConn(t)
	defer checkPing(t, c) // ensure we leave in a good state

	_, err := c.Exec(context.Background(), `
		CREATE FUNCTION raise_notice() RETURNS integer
		   LANGUAGE plpgsql AS
		$$BEGIN
		   RAISE NOTICE 'boom';
		   RETURN 42;
		END;$$;
	`)
	if err != nil {
		t.Fatal(err)
	}

	notice := func(ctx context.Context) {
		t.Helper()
		_, err = c.Exec(ctx, "SELECT raise_notice()")
		if err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()

	// no handler
	notice(ctx)
	checkPing(t, c) // check notice is dropped

	var got []*febe.Notice
	ctx = febe.WithTrace(context.Background(), &febe.Trace{
		ReceivedNotice: func(ctx context.Context, notice *febe.Notice) {
			got = append(got, notice)
		},
	})

	notice(ctx)
	want := []*febe.Notice{
		{
			Severity:  "NOTICE",
			Verbosity: "NOTICE",
			Message:   "boom",
		},
	}
	keep := diff.KeepFields[febe.Notice]("Severity", "Verbosity", "Message")
	diff.Test(t, t.Errorf, got, want, keep)

}

func TestParameterStatus(t *testing.T) {
	c := newConn(t)
	defer checkPing(t, c) // ensure we leave in a good state

	var got []febe.Parameter
	ctx := febe.WithTrace(context.Background(), &febe.Trace{
		ReceivedParameterStatus: func(ctx context.Context, p febe.Parameter) {
			got = append(got, p)
		},
	})

	_, err := c.Exec(ctx, "SET datestyle TO postgres, dmy")
	if err != nil {
		t.Fatal(err)
	}

	checkPing(t, c) // flush out parameter status if not received by Exec

	diff.Test(t, t.Errorf, got, []febe.Parameter{
		{Name: "DateStyle", Value: "Postgres, DMY"},
	})
}

func BenchmarkQuery(b *testing.B) {
	ctx := context.Background()

	b.Run("Simple", func(b *testing.B) {
		c := newConn(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := c.Query(ctx, "SELECT 1").Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Extended", func(b *testing.B) {
		c := newConn(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := c.Query(ctx, "SELECT $1", "1").Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPGX(b *testing.B) {
	dsn := createDB(b)

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(context.Background())

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err := conn.QueryRow(context.Background(), "SELECT 1").Scan(nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func checkPing(t *testing.T, c *Conn) {
	t.Helper()
	rs := c.Query(context.Background(), "SELECT 1")
	var ok bool
	for row, err := range rs.All {
		if ok {
			t.Fatal("got more than one row")
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := row.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "1" {
			t.Fatalf("got %q, want %q", data, "1")
		}
		ok = true
	}
}

func mustGetUsername() string {
	cu, err := user.Current()
	if err != nil {
		panic(err)
	}
	return cu.Username
}

var (
	mainDSN = "dbname=postgres port=5433 sslmode=disable user=" + mustGetUsername()

	mc = func() *Conn {
		c, err := Dial(context.Background(), mainDSN)
		if err != nil {
			panic(err)
		}
		return c
	}()
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
