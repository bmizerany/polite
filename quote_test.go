package polite

import (
	"bytes"
	"strings"
	"testing"
)

var quoteCases = []struct {
	name string
	in   string
	want string
}{
	{"empty", "", "''"},
	{"plain", "plain", "'plain'"},
	{"space", "hello world", "'hello world'"},
	{"only_apos", "'", "''''"},
	{"leading_apos", "'x", "'''x'"},
	{"trailing_apos", "x'", "'x'''"},
	{"two_apos", "''", "''''''"},
	{"three_apos", "'''", "''''''''"},
	{"its", "it's", "'it''s'"},
	{"backslash", `\`, `'\'`},
	{"backslash_apos", `\'`, `'\'''`},
	{"apos_backslash", `'\`, `'''\'`},
	{"backslash_n", `\n`, `'\n'`},
	{"double_backslash", `\\`, `'\\'`},
	{"nul", "a\x00b", "'a\x00b'"},
	{"controls", "\t\n\r\x07", "'\t\n\r\x07'"},
	{"utf8", "héllo café 🙂", "'héllo café 🙂'"},
	{"utf8_apos", "café's", "'café''s'"},
}

func TestQuote(t *testing.T) {
	for _, tc := range quoteCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Quote(tc.in); got != tc.want {
				t.Errorf("Quote(string) = %q, want %q", got, tc.want)
			}
			if got := Quote([]byte(tc.in)); got != tc.want {
				t.Errorf("Quote([]byte) = %q, want %q", got, tc.want)
			}
		})
	}

	t.Run("long", func(t *testing.T) {
		s := strings.Repeat("a'b", 2048) // > 4096 bytes, lots of apostrophes
		got := Quote(s)
		if got[0] != '\'' || got[len(got)-1] != '\'' {
			t.Fatal("missing framing apostrophes")
		}
		if u, ok := unquote(got); !ok || u != s {
			t.Fatalf("round-trip failed")
		}
	})

	t.Run("named_string", func(t *testing.T) {
		type myStr string
		if got := Quote(myStr("it's")); got != "'it''s'" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("named_bytes", func(t *testing.T) {
		type myBytes []byte
		if got := Quote(myBytes("it's")); got != "'it''s'" {
			t.Errorf("got %q", got)
		}
	})
}

// unquote reverses Quote: strip framing apostrophes and collapse '' to '.
// Returns false if the input is not a well-formed Quote output.
func unquote(s string) (string, bool) {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return "", false
	}
	inner := s[1 : len(s)-1]
	var b strings.Builder
	b.Grow(len(inner))
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\'' {
			if i+1 >= len(inner) || inner[i+1] != '\'' {
				return "", false
			}
			b.WriteByte('\'')
			i++
			continue
		}
		b.WriteByte(inner[i])
	}
	return b.String(), true
}

func FuzzQuote(f *testing.F) {
	for _, tc := range quoteCases {
		f.Add(tc.in)
	}
	f.Add("\xff\xfe\x00")
	f.Add(strings.Repeat("'", 1024))
	f.Add(strings.Repeat("a'", 2048))
	f.Add("\\\\\\'")

	f.Fuzz(func(t *testing.T, in string) {
		out := Quote(in)
		if len(out) < 2 || out[0] != '\'' || out[len(out)-1] != '\'' {
			t.Fatalf("Quote(%q) = %q: missing framing apostrophes", in, out)
		}
		inner := out[1 : len(out)-1]
		if bytes.Count([]byte(inner), []byte{'\''})%2 != 0 {
			t.Fatalf("Quote(%q) = %q: odd number of interior apostrophes", in, out)
		}
		got, ok := unquote(out)
		if !ok {
			t.Fatalf("Quote(%q) = %q: not a well-formed quoted literal", in, out)
		}
		if got != in {
			t.Fatalf("round-trip: Quote(%q) -> %q -> %q", in, out, got)
		}
		// Also exercise the []byte path.
		if Quote([]byte(in)) != out {
			t.Fatalf("Quote([]byte) disagrees with Quote(string) for %q", in)
		}
	})
}
