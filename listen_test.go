package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"kr.dev/diff"
)

func ExampleListener() {
	ctx := context.Background()

	l, err := OpenListener(ctx, "host=localhost port=5433 dbname=postgres", nil)
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
		fmt.Println("NOTIFICATION:", string(n.Channel), string(n.Payload))
	}
}

func TestListenNotify(t *testing.T) {
	dsn := createDB(t)
	ctx := context.Background()

	cn, err := Open(ctx, dsn, nil)
	if err != nil {
		t.Error(err)
	}

	l, err := OpenListener(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Listen(ctx, "foo"); err != nil {
		t.Fatal(err)
	}

	if err := cn.Exec(ctx, "NOTIFY foo, 'bar'"); err != nil {
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
		Channel: []byte("foo"),
		Payload: []byte("bar"),
	}
	diff.Test(t, t.Errorf, got, want)
}
