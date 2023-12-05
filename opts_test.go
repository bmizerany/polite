package postgres

import (
	"testing"

	"golang.org/x/exp/slices"
	"kr.dev/diff"
)

func TestParseOptions(t *testing.T) {
	cases := []struct {
		dsn  string
		want Options
	}{
		{"dbname=test", Options{Host: "localhost", Port: "5432", Extra: []string{"database=test"}}},
		{"dbname=test user=foo", Options{Host: "localhost", Port: "5432", Extra: []string{"database=test", "user=foo"}}},
		{"dbname=test user=foo password=bar", Options{Host: "localhost", Port: "5432", Extra: []string{"database=test", "password=bar", "user=foo"}}},
		{"user=foo", Options{Host: "localhost", Port: "5432", Extra: []string{"user=foo"}}},
		{"sslmode=foo", Options{Host: "localhost", Port: "5432", SSLMode: "foo"}},
		{"postgres://foo:bar@localhost:5432/test", Options{Host: "localhost", Port: "5432"}},
		{"postgres:///test", Options{Host: "localhost", Port: "5432"}},

		{"a=x b=y", Options{Host: "localhost", Port: "5432", Extra: []string{"a=x", "b=y"}}},
		{"postgres://foo:bar@localhost:5432/test?a=x&b=y", Options{Host: "localhost", Port: "5432",
			Extra: []string{"a=x", "b=y"}}},
	}

	for _, c := range cases {
		t.Run(c.dsn, func(t *testing.T) {
			got, err := ParseOptions(c.dsn)
			if err != nil {
				t.Fatal(err)
			}
			slices.Sort(got.Extra)
			if c.want.SSLMode == "" {
				c.want.SSLMode = "verify"
			}
			diff.Test(t, t.Errorf, got, c.want)
		})
	}
}
