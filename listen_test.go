package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go4.org/mem"
	"kr.dev/diff"
)

var compareMem = diff.Transform(func(s mem.RO) any {
	return s.StringCopy()
})

func ExampleListener() {
	ctx := context.Background()

	l, err := DialListener(ctx, "host=localhost port=5433 dbname=postgres", nil)
	if err != nil {
		panic(err)
	}
	defer l.Close()

	if err := l.Listen(ctx, "foo"); err != nil {
		panic(err)
	}

	for n, err := range l.Notifications(ctx) {
		if err != nil {
			panic(err)
		}
		fmt.Println("NOTIFICATION:", n.Channel.StringCopy(), n.Payload.StringCopy())
	}
}

func TestListenNotify(t *testing.T) {
	dsn := createDB(t)
	ctx := context.Background()

	c, err := Dial(ctx, dsn, nil)
	if err != nil {
		t.Error(err)
	}

	l, err := DialListener(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Listen(ctx, "foo"); err != nil {
		t.Fatal(err)
	}

	if err := c.Exec(ctx, "NOTIFY foo, 'bar'"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var got Notification
	for n, err := range l.Notifications(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		got = n
		break
	}
	if got.PID == 0 {
		t.Error("expected non-zero PID")
	}
	got.PID = 0
	want := Notification{
		Channel: mem.S("foo"),
		Payload: mem.S("bar"),
	}
	diff.Test(t, t.Errorf, got, want, compareMem)
}
