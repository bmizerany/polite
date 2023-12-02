package pqlite

import (
	"context"
	"testing"
	"time"
)

func TestListenNotify(t *testing.T) {
	dsn := createDB(t)
	ctx := context.Background()

	c, err := Dial(ctx, dsn)
	if err != nil {
		t.Error(err)
	}

	l, err := DialListener(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Listen(ctx, "foo"); err != nil {
		t.Fatal(err)
	}

	checkExec(t, c, "NOTIFY foo, 'bar'")

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	for got, err := range l.Notifications(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("notification: %+v", got)
		if string(got.Channel) != "foo" {
			t.Errorf("got = %q, want %q", got.Channel, "foo")
		}
		if string(got.Payload) != "bar" {
			t.Errorf("got = %q, want %q", got.Payload, "bar")
		}
		break
	}
}
