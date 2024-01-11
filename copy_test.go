package postgres

import (
	"context"
	"testing"

	"blake.io/postgres/febe"
	"kr.dev/diff"
)

func TestStream(t *testing.T) {
	dsn := createDB(t)

	ctx := context.Background()
	ctx = febe.WithTrace(ctx, &febe.Trace{
		ReceivedHeader: func(ctx context.Context, h febe.MessageHeader) {
			t.Logf("received header: %v", h)
		},
	})

	c, err := Dial(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Exec(ctx, "CREATE TABLE foo (id INT, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Exec(ctx, "INSERT INTO foo VALUES (1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		t.Fatal(err)
	}

	type G struct {
		ID   int64
		Name string
	}

	var got []G
	var i int
	for row, err := range c.CopyTo(ctx, "foo") {
		if err != nil {
			t.Fatalf("[%d] %v", i, err)
		}
		id, err := row.ParseInt(10, 64)
		if err != nil {
			t.Fatal(err)
		}
		name, err := row.View()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, G{ID: id, Name: name.StringCopy()})
		i++
	}

	want := []string{
		"1", "foo",
		"2", "bar",
		"3", "baz",
	}

	diff.Test(t, t.Errorf, got, want)
}
