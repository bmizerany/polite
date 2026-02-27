package pqlite

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
)

// CopyError is returned by a CopyFrom input iterator to send an explicit
// message to PostgreSQL via COPY FAIL.
//
// By default, CopyFrom does not forward iterator error messages to the server
// to avoid leaking sensitive frontend details. Wrap an iterator error with
// CopyError when you want that exact message sent to PostgreSQL.
type CopyError string

func (e CopyError) Error() string { return string(e) }

// CopyTo streams rows from PostgreSQL using COPY ... TO STDOUT WITH (FORMAT binary).
//
// table is the source table name. columns is optional; nil or empty means all
// columns, otherwise only the listed columns are copied in the given order.
//
// The returned iterator yields one [Row] per copied row, with raw PostgreSQL
// binary column values.
//
// As with [Conn.Query], each yielded row must be fully consumed before
// continuing iteration. If you exit early, call [Row.Close] on the current row.
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

// CopyFrom streams rows to PostgreSQL using COPY ... FROM STDIN WITH (FORMAT binary).
//
// table is the target table name. columns is optional; nil or empty means all
// columns in table order, otherwise values are mapped to the listed columns.
//
// from yields one row per iteration as raw PostgreSQL binary column values. If
// from yields an error, CopyFrom stops and returns it.
//
// Iterator errors are not sent to PostgreSQL by default. This avoids
// accidentally exposing frontend/internal details (for example credentials,
// SQL text, or stack context) in PostgreSQL-visible errors or logs. Wrap an
// error with [CopyError] when you want to intentionally send a sanitized
// operator-facing message via COPY FAIL.
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
