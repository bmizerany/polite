package pqlite

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
)

// CopyError is an error that can be returned by an iter passed into
// CopyFrom that will be used as the message sent to the server when a COPY
// FROM fails.
//
// By default, CopyFrom does not send the error message to the server to
// avoid accidentally sending sensitive information. If you want to send the
// error message to the server, you can use the CopyError type to wrap the
// error and pass it to CopyFrom via the iter.
type CopyError string

func (e CopyError) Error() string { return string(e) }

func (c *Conn) CopyTo(ctx context.Context, table string, columns []string) iter.Seq2[*Row, error] {
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
	return c.Query(ctx, b.String())
}

func (c *Conn) CopyFrom(ctx context.Context, table string, columns []string, from iter.Seq2[[][]byte, error]) error {
	if err := c.copyFrom(ctx, table, columns, from); err != nil {
		var e *CopyError
		if errors.As(err, &e) {
			c.fc.WriteCopyFail(e.Error())
		} else {
			c.fc.WriteCopyFail("pqlite: frontend error copying data; see frontend logs for details")
		}
		return err
	}
	return nil
}

func (c *Conn) copyFrom(ctx context.Context, table string, columns []string, from iter.Seq2[[][]byte, error]) error {
	var b strings.Builder
	b.WriteString("COPY ")
	writeQuotedIdentifier(&b, table)
	if len(columns) > 0 {
		b.WriteString(" (")
		for i, col := range columns {
			if i > 0 {
				b.WriteString(", ")
			}
			writeQuotedIdentifier(&b, col)
		}
		b.WriteString(")")
	}
	b.WriteString(" FROM STDIN")
	b.WriteString(" WITH (FORMAT binary)")

	if err := c.prepare(ctx, "", b.String()); err != nil {
		return err
	}
	if err := c.execAndSync(""); err != nil {
		return err
	}

	err := func() error {
		for {
			h, err := c.fc.ReadHeader(ctx)
			if err != nil {
				return err
			}
			switch h.Type() {
			case '2':
				continue
			case 'G': // CopyInResponse
				_, err := c.fc.ReadCopyIn()
				return err
			}
		}
	}()
	if err != nil {
		return err
	}

	var wroteHeader bool
	for values, err := range from {
		if err != nil {
			return err
		}
		if err := c.fc.WriteCopyDataRow(values, !wroteHeader); err != nil {
			return err
		}
		wroteHeader = true
	}
	c.fc.WriteCopyDataRow(nil, false)
	c.fc.WriteCopyDone()
	c.fc.WriteSync()
	if err := c.fc.Flush(); err != nil {
		return err
	}

	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return c.maybeRecoverFrom(ctx, err)
		}
		switch h.Type() {
		case 'C':
			m, err := c.fc.ReadCommandComplete()
			if err != nil {
				return err
			}
			c.rowsAffected = int(m.RowsAffected)
		case 'Z':
			_, err := c.fc.ReadReadyForQuery()
			return err
		default:
			return fmt.Errorf("unexpected message after copy: %q", h.Type())
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
