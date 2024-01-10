package postgres

import (
	"context"
	"strings"

	"blake.io/postgres/iter"
)

func (c *Conn) CopyTo(ctx context.Context, table string, columns ...string) iter.Seq2[*Row, error] {
	var b strings.Builder
	b.WriteString("COPY ")
	writeQuotedIdentifier(&b, table)
	b.WriteString(" ")
	if len(columns) > 0 {
		b.WriteString("(")
		for i, col := range columns {
			if i > 0 {
				b.WriteString(", ")
			}
			writeQuotedIdentifier(&b, col)
		}
		b.WriteString(")")
	}
	b.WriteString(" TO STDIN")
	b.WriteString(" WITH (FORMAT binary)")
	return c.QueryAll(ctx, b.String())
}

func writeQuotedIdentifier(b *strings.Builder, name string) {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	for _, c := range name {
		if c == '"' {
			b.WriteRune('"')
		}
		b.WriteRune(c)
	}
}
