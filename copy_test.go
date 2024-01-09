package postgres

import (
	"context"
	"testing"

	"kr.dev/diff"
)

func TestStream(t *testing.T) {
	dsn := createDB(t)

	ctx := context.Background()
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

	var got []string
	for row, err := range c.Stream(ctx, "COPY foo TO STDOUT").Out {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, row.StringCopy())
	}

	want := []string{
		"1\tfoo\n",
		"2\tbar\n",
		"3\tbaz\n",
	}

	diff.Test(t, t.Errorf, got, want)
}
