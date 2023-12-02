package pqlite

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os/exec"
	"os/user"
	"reflect"
	"testing"
)

func newConn(t *testing.T) *Conn {
	t.Helper()
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
	for row, err := range c.Query(context.Background(), "SELECT 1, 2, 'three'") {
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

func TestQueryEmptyQueryResponse(t *testing.T) {
	c := newConn(t)
	var got error
	for _, got = range c.Query(context.Background(), "") {
		break
	}
	if !errors.Is(got, ErrEmptyQuery) {
		t.Errorf("got = %v, want %v", got, ErrEmptyQuery)
	}

	// check there is no residue left on the wire
	checkPing(t, c)
}

func checkPing(t *testing.T, c *Conn) {
	t.Helper()
	for row, err := range c.Query(context.Background(), "SELECT 1") {
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
	for row, err := range c.Query(context.Background(), sql) {
		if err != nil {
			t.Fatal(err)
		}
		t.Error("should not have received a row")
		for _, b := range row.Values() {
			t.Logf("value: len(b) = %d: %s", len(b), b)
		}
	}
}

func mustGetUsername() string {
	cu, err := user.Current()
	if err != nil {
		panic(err)
	}
	return cu.Username
}

func createDB(t *testing.T) (dsn string) {
	t.Helper()
	// Boostrap with `createdb` command.
	//
	// TODO(bmizerany): Replace use of createdb with our driver once
	// enough meat is on the bones to make it work.
	dbname := fmt.Sprintf("%s_%s", t.Name(), randomString())
	cmd := exec.Command("createdb", dbname)
	t.Logf("createDB: running: %s", cmd)
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			t.Logf("createDB: createdb exit code: %d: %s", ee.ExitCode(), ee.Stderr)
		} else {
			t.Logf("createDB: createdb error: %s", err)
		}
		t.Logf("createDB: createdb output: %s", cmp.Or(string(out), "<EMPTY>"))
		t.FailNow()
	}
	dsn = "user=" + mustGetUsername() + " dbname=" + dbname + " sslmode=disable"
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
