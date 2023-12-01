package pqlite

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os/exec"
	"os/user"
	"testing"
)

func TestQuery(t *testing.T) {
	dsn := createDB(t)
	ctx := context.Background()

	c, err := Dial(ctx, dsn)
	if err != nil {
		t.Error(err)
	}

	r, err := c.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Error(err)
	}
	defer r.Close()

	for {
		r, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range r.Values() {
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
