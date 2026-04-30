package polite

import "strings"

// Quote returns v as a SQL string literal: wrapped in apostrophes, with
// each interior apostrophe doubled. PostgreSQL with the default
// standard_conforming_strings=on treats backslash as a literal byte
// inside '...', so the apostrophe is the only character that needs
// escaping. Quote is byte-oriented and safe for any UTF-8 input.
//
// Quote is for SQL string literals, not identifiers. Pass user data as
// a $N parameter when possible; reach for Quote only when a literal
// must appear directly in the SQL text.
func Quote[T ~string | ~[]byte](v T) string {
	s := string(v)
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			n++
		}
	}
	var b strings.Builder
	b.Grow(len(s) + n + 2)
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' {
			b.WriteByte('\'')
		}
		b.WriteByte(c)
	}
	b.WriteByte('\'')
	return b.String()
}
