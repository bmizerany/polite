package pqlite

import (
	"context"
	"os/user"
	"strings"
	"testing"

	"blake.io/pqx/pqxtest"
	_ "github.com/lib/pq"
)

func TestMain(m *testing.M) {
	pqxtest.TestMain(m)
}

func TestConnDial(t *testing.T) {
	pqxtest.CreateDB(t, ``)
	dsn := pqxtest.DSNForTest(t) + " user=" + mustGetUsername()
	dsn = strings.Replace(dsn, "dbname=", "database=", -1)
	t.Logf(dsn)
	ctx := context.Background()
	_, err := Dial(ctx, dsn)
	if err != nil {
		t.Error(err)
	}
}

func mustGetUsername() string {
	cu, err := user.Current()
	if err != nil {
		panic(err)
	}
	return cu.Username
}
