package pqlite

import (
	"context"
	"testing"

	"blake.io/pqx/pqxtest"
	_ "github.com/lib/pq"
)

func TestMain(m *testing.M) {
	pqxtest.TestMain(m)
}

func TestConnDial(t *testing.T) {
	pqxtest.CreateDB(t, ``)
	dsn := pqxtest.DSNForTest(t)
	ctx := context.Background()
	_, err := Dial(ctx, dsn)
	if err != nil {
		t.Error(err)
	}
}
