package pqlite

import (
	"testing"

	"kr.dev/diff"
)

func TestOptsParse(t *testing.T) {
	cases := []struct {
		dsn  string
		want Options
	}{
		{"dbname=test", Options{DBName: "test", Host: "localhost", Port: "5432", User: "postgres", SSLMode: "verify"}},
		{"dbname=test user=foo", Options{DBName: "test", User: "foo", Host: "localhost", Port: "5432", SSLMode: "verify"}},
		{"dbname=test user=foo password=bar", Options{DBName: "test", User: "foo", Password: "bar", Host: "localhost", Port: "5432", SSLMode: "verify"}},
		{"user=foo", Options{User: "foo", DBName: "postgres", Host: "localhost", Port: "5432", SSLMode: "verify"}},
		{"sslmode=foo", Options{SSLMode: "foo", DBName: "postgres", User: "postgres", Host: "localhost", Port: "5432"}},
		{"postgres://foo:bar@localhost:5432/test", Options{User: "foo", Password: "bar", Host: "localhost", Port: "5432", DBName: "test", SSLMode: "verify"}},
		{"postgres:///test", Options{DBName: "test", Host: "localhost", Port: "5432", User: "postgres", SSLMode: "verify"}},
	}

	for _, c := range cases {
		t.Run(c.dsn, func(t *testing.T) {
			got, err := ParseOptions(c.dsn)
			if err != nil {
				t.Fatal(err)
			}
			diff.Test(t, t.Errorf, got, c.want)
		})
	}
}
