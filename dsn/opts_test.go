package dsn

import (
	"slices"
	"strings"
	"testing"

	"kr.dev/diff"
)

func FuzzScanDSN(f *testing.F) {
	f.Add("host=localhost port=5432")
	f.Add("dbname='my db' user=foo")
	f.Add("key='it\\'s escaped'")
	f.Add("key='escaped at end\\\\'")
	f.Add("a = b")
	f.Add("===")
	f.Add("")
	f.Add("  \t\n  ")
	f.Add("'unclosed")
	f.Add("''")
	f.Add("a=b c=d e=f")

	f.Fuzz(func(t *testing.T, dsn string) {
		var tokens []string
		var scanErr error
		for tok, err := range ScanDSN(dsn) {
			if err != nil {
				scanErr = err
				break
			}
			tokens = append(tokens, tok)
		}

		if scanErr != nil {
			if !strings.Contains(scanErr.Error(), "unclosed quoted string") {
				t.Fatalf("unexpected error: %v", scanErr)
			}
			return
		}

		for i, tok := range tokens {
			if tok == "" {
				t.Fatalf("token %d is empty", i)
			}
			if tok == "=" {
				continue
			}
			if tok[0] == '\'' {
				if tok[len(tok)-1] != '\'' {
					t.Fatalf("token %d: %q starts with quote but does not end with one", i, tok)
				}
				continue
			}
			for j := 0; j < len(tok); j++ {
				c := tok[j]
				if c == '=' || isWhitespace[c] {
					t.Fatalf("token %d: unquoted %q contains whitespace or '=' at byte %d", i, tok, j)
				}
			}
		}

		// Round-trip: joining with spaces and re-scanning must
		// produce the same token sequence.
		joined := strings.Join(tokens, " ")
		var tokens2 []string
		for tok, err := range ScanDSN(joined) {
			if err != nil {
				t.Fatalf("round-trip scan of %q failed: %v", joined, err)
			}
			tokens2 = append(tokens2, tok)
		}
		if !slices.Equal(tokens, tokens2) {
			t.Fatalf("round-trip mismatch:\n  got:  %q\n  want: %q", tokens2, tokens)
		}
	})
}

func TestParseOptions(t *testing.T) {
	cases := []struct {
		dsn  string
		want *Options
	}{
		{"dbname=test", &Options{
			User:     defaultUser,
			Host:     "/tmp",
			Port:     "5432",
			Database: "test",
			Extra:    []string{"user=" + defaultUser, "database=test"},
		}},
		{"dbname=test user=foo", &Options{
			Host:     "/tmp",
			Port:     "5432",
			User:     "foo",
			Database: "test",
			Extra:    []string{"user=foo", "database=test"},
		}},
		{"dbname=test user=foo password=bar", &Options{
			Host:     "/tmp",
			Port:     "5432",
			User:     "foo",
			Password: "bar",
			Database: "test",

			// password is NOT to be included in the Extra field
			Extra: []string{"database=test", "user=foo"},
		}},
		{"user=foo", &Options{
			Host:     "/tmp",
			Port:     "5432",
			User:     "foo",
			Database: "postgres",
			Extra:    []string{"user=foo", "database=postgres"},
		}},
		{"sslmode=foo", &Options{
			Host:     "/tmp",
			Port:     "5432",
			User:     defaultUser,
			Database: "postgres",
			SSLMode:  "foo",
			Extra:    []string{"user=" + defaultUser, "database=postgres"},
		}},
		{"a=x b=y", &Options{
			Host:     "/tmp",
			Port:     "5432",
			User:     defaultUser,
			Database: "postgres",
			Extra:    []string{"a=x", "b=y", "user=" + defaultUser, "database=postgres"},
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
			Host:     "/tmp",
			Port:     "5432",
			User:     defaultUser,
			Database: "test",
			Extra:    []string{"user=" + defaultUser, "database=test"},
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
			Host:        "/tmp",
			Port:        "5432",
			User:        defaultUser,
			Database:    "postgres",
			SSLRootCert: "foo",
			Extra:       []string{"user=" + defaultUser, "database=postgres"},
		}},
		{"quote='foo bar'", &Options{
			Host:     "/tmp",
			Port:     "5432",
			User:     defaultUser,
			Database: "postgres",
			Extra:    []string{"quote='foo bar'", "user=" + defaultUser, "database=postgres"},
		}},
		{"postgres://foo:bar@localhost:5432/test?sslmode=disable", &Options{
			Host:     "localhost",
			Port:     "5432",
			User:     "foo",
			Password: "bar",
			Database: "test",
			SSLMode:  "disable",
			Extra:    []string{"database=test", "user=foo"},
		}},

		// make sure we acknowledge the scheme "postgresql" too.
		{"postgresql://u:p@localhost", &Options{
			Host:     "localhost",
			Port:     "5432",
			User:     "u",
			Password: "p",
			Database: "postgres",
			Extra:    []string{"user=u", "database=postgres"},
		}},
	}

	for _, tt := range cases {
		t.Run(tt.dsn, func(t *testing.T) {
			got, err := ParseOptions(tt.dsn)
			if err != nil {
				t.Fatal(err)
			}
			if tt.want.SSLMode == "" {
				tt.want.SSLMode = "verify-full"
			}

			slices.Sort(got.Extra)
			slices.Sort(tt.want.Extra)
			diff.Test(t, t.Errorf, got, tt.want)
		})
	}
}

func TestURIUnsupportedScheme(t *testing.T) {
	cases := []string{
		"mysql://foo:bar@localhost:5432/test",
		"f://foo:bar@localhost:5432/test",
		"file:///tmp",
	}
	for _, dsn := range cases {
		t.Run(dsn, func(t *testing.T) {
			_, err := ParseOptions(dsn)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), "unsupported scheme") {
				t.Fatalf("er = %v; want unsupported scheme", err)
			}
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
