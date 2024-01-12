package postgres

import (
	"context"
	"encoding/binary"
	"slices"
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
	b.WriteString(" TO STDOUT")
	b.WriteString(" WITH (FORMAT binary)")
	return c.QueryAll(ctx, b.String())
}

type Stream struct {
	ctx     context.Context
	c       *Conn
	numCols int
	scatch  []byte
}

func (s *Stream) WriteBytes(b []byte) error {
	return s.c.fc.WriteCopyDataBinaryValue(s.ctx, b)
}

func (s *Stream) WriteInt64(n int64) error {
	s.scatch = slices.Grow(s.scatch, 8)
	s.scatch = binary.BigEndian.AppendUint64(s.scatch, uint64(n))
	return s.WriteBytes(s.scatch[:8])
}

func (s *Stream) Reset() error {
	_, err := s.c.fc.WriteCopyDataBinaryNumColumns(s.ctx, s.numCols)
	return err
}

func (s *Stream) Close() error {
	return s.CloseWithError(nil)
}

func (s *Stream) CloseWithError(err error) error {
	if err == nil {
		_, err := s.c.fc.WriteCopyDone()
		return err
	}
	_, err = s.c.fc.WriteCopyFail(err.Error())
	return err
}

func (c *Conn) CopyFrom(ctx context.Context, table string, columns ...string) iter.Seq2[*Row, error] {
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
	b.WriteString(" FROM STDIN")
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
			b.WriteString("\"\"")
		}
		b.WriteRune(c)
	}
}
