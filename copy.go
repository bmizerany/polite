package postgres

import (
	"context"
	"fmt"
	"iter"
	"strings"
)

type CopyRow struct {
	err error
}

func (c *Conn) CopyTo(ctx context.Context, table string, columns ...string) iter.Seq2[*CopyRow, error] {
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
	b.WriteString(" TO STDOUT")
	b.WriteString(" WITH (FORMAT binary)")

	return func(yield func(*CopyRow, error) bool) {
		if err := c.prepare(ctx, "", b.String()); err != nil {
			yield(nil, err)
			return
		}
		if err := c.execAndSync(ctx, ""); err != nil {
			yield(nil, err)
			return
		}
		for {
			h, err := c.fc.ReadHeader(ctx)
			if err != nil {
				yield(nil, err)
				return
			}
			if h.Type() == 'd' {
				cd, err := c.fc.ReadCopyData(ctx)
				if err != nil {
					yield(nil, err)
					return
				}
				_ = cd // TODO(bmizerany): use this
				if !yield(&CopyRow{}, nil) {
					return
				}
				continue
			} else if h.Type() == 'c' {
				return
			} else {
				yield(nil, fmt.Errorf("unexpected message type %q", h.Type()))
				return
			}
		}
	}
}

func writeQuotedIdentifier(b *strings.Builder, name string) {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	for _, c := range name {
		if c == '"' {
			b.WriteString("\"\"")
		}
		b.WriteRune(c)
	}
}
