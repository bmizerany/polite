package pqlite

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os/user"
	"reflect"
	"strings"
	"testing"

	"kr.dev/diff"
)

func newConn(t *testing.T) *Conn {
	dsn := createDB(t)
	ctx := context.Background()
	c, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	})
	checkPing(t, c)
	return c
}

func TestQuery(t *testing.T) {
	c := newConn(t)
	rs := c.Query(context.Background(), "SELECT 1, 2, 'three'")
	for row, err := range rs.All {
		if err != nil {
			t.Fatal(err)
		}
		got := row.Values()
		want := [][]byte{{'1'}, {'2'}, {'t', 'h', 'r', 'e', 'e'}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got = %v, want %v", got, want)
		}
	}
}

func TestQuerySelectTag(t *testing.T) {
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

func TestQueryEmptyQueryResponse(t *testing.T) {
	c := newConn(t)
	var got error
	rs := c.Query(context.Background(), "")
	for _, got = range rs.All {
		break
	}
	if !errors.Is(got, ErrEmptyQuery) {
		t.Errorf("got = %v, want %v", got, ErrEmptyQuery)
	}

	// check there is no residue left on the wire
	checkPing(t, c)
}

func TestExec(t *testing.T) {
	cn := newConn(t)
	checkExec(t, cn, "CREATE TABLE foo (n INT)")
	got, err := cn.Exec(context.Background(), "INSERT INTO foo VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	want := Result{
		RowsAffected: 1,
	}
	diff.Test(t, t.Errorf, got, want)
}

func checkPing(t *testing.T, c *Conn) {
	t.Helper()
	rs := c.Query(context.Background(), "SELECT 1")
	for row, err := range rs.All {
		if err != nil {
			t.Fatal(err)
		}
		got := row.Values()
		want := [][]byte{{'1'}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got = %v, want %v", got, want)
		}
	}
}

func checkExec(t *testing.T, c *Conn, sql string) {
	t.Helper()
	t.Logf("exec: %q", sql)
	_, err := c.Exec(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
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

func createDB(t *testing.T) (dsn string) {
	dbname := fmt.Sprintf("test_%s_%s", t.Name(), randomString())
	dbname = strings.ToLower(dbname)
	checkExec(t, mc, fmt.Sprintf("CREATE DATABASE %s", dbname))
	dsn = mainDSN + " dbname=" + dbname
	t.Logf("createDB: dsn: %q", dsn)
	return dsn
}

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
