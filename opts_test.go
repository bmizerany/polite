package postgres

import (
	"testing"

	"golang.org/x/exp/slices"
	"kr.dev/diff"
)

func TestParseOptions(t *testing.T) {
	cases := []struct {
		dsn  string
		want *Options
	}{
		{"dbname=test", &Options{
			Host:  "localhost",
			Port:  "5432",
			Extra: []string{"database=test"},
		}},
		{"dbname=test user=foo", &Options{
			Host:  "localhost",
			Port:  "5432",
			Extra: []string{"database=test", "user=foo"},
		}},
		{"dbname=test user=foo password=bar", &Options{
			Host:  "localhost",
			Port:  "5432",
			Extra: []string{"database=test", "password=bar", "user=foo"},
		}},
		{"user=foo", &Options{
			Host:  "localhost",
			Port:  "5432",
			Extra: []string{"user=foo"},
		}},
		{"sslmode=foo", &Options{
			Host:    "localhost",
			Port:    "5432",
			SSLMode: "foo",
		}},
		{"a=x b=y", &Options{
			Host:  "localhost",
			Port:  "5432",
			Extra: []string{"a=x", "b=y"},
		}},

		// URL options
		{"postgres://foo:bar@localhost:5432/test", &Options{
			Host:     "localhost",
			Port:     "5432",
			User:     "foo",
			Password: "bar",
			Database: "test",
			Extra:    []string{"database=test", "user=foo"},
		}},
		{"postgres:///test", &Options{
			Host:     "localhost",
			Port:     "5432",
			Database: "test",
			Extra:    []string{"user=", "database=test"},
		}},
		{"postgres://foo:bar@localhost:5432/test?a=x&b=y", &Options{
			Host:     "localhost",
			Port:     "5432",
			User:     "foo",
			Password: "bar",
			Database: "test",
			Extra:    []string{"a=x", "b=y", "database=test", "user=foo"},
		}},
		{"sslrootcert=foo", &Options{
			Host:        "localhost",
			Port:        "5432",
			SSLRootCert: "foo",
		}},
	}

	for _, tt := range cases {
		t.Run(tt.dsn, func(t *testing.T) {
			got, err := ParseOptions(tt.dsn)
			if err != nil {
				t.Fatal(err)
			}
			if tt.want.SSLMode == "" {
				tt.want.SSLMode = "require"
			}

			slices.Sort(got.Extra)
			slices.Sort(tt.want.Extra)
			diff.Test(t, t.Errorf, got, tt.want)
		})
	}
}

func TestScanDSN(t *testing.T) {
	const dsn = `
		dbname=test
		user=foo
		password=bar
		sslmode=disable
		host=localhost
		port=5432
		somequote='foo bar'
		escapequote='foo\'bar'
		x=y
	`

	var got []string
	for tok, err := range ScanDSN(dsn) {
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("tok: %q", tok)
		got = append(got, tok)
	}
	want := []string{
		"dbname", "=", "test",
		"user", "=", "foo",
		"password", "=", "bar",
		"sslmode", "=", "disable",
		"host", "=", "localhost",
		"port", "=", "5432",
		"somequote", "=", "'foo bar'",
		"escapequote", "=", "'foo\\'bar'",
		"x", "=", "y",
	}

	t.Logf("got: %q", got)

	diff.Test(t, t.Errorf, got, want)
}
