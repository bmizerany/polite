package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go4.org/mem"
)

type Stream struct {
	c            *Conn
	ctx          context.Context
	err          error
	rowsAffected int64
}

// Stream executes a query and streams the data via a COPY
func (c *Conn) Stream(ctx context.Context, sql string, args ...mem.RO) *Stream {
	if err := c.prepare(ctx, sql, args...); err != nil {
		return &Stream{err: err}
	}
	return &Stream{c: c, ctx: ctx}
}

type CopyRow struct {
	ctx context.Context
	v   mem.RO
	rem int
}

func (r *CopyRow) Unprocessed() int {
	return r.rem
}

func (st *Stream) Out(yield func(*CopyRow, error) bool) {
	if st.err != nil {
		yield(nil, st.err)
		return
	}
	if err := st.c.fc.WriteExecute(st.ctx, "", 0); err != nil {
		yield(nil, err)
		return
	}
	if err := st.c.fc.WriteSync(st.ctx); err != nil {
		yield(nil, err)
		return
	}
	if err := st.c.fc.Flush(); err != nil {
		yield(nil, err)
		return
	}

	for {
		row, err := st.next()
		if err != nil {
			st.err = err
			if errors.Is(err, ErrDone) {
				return
			}
			yield(zero, err)
			return
		}
		if !yield(row, nil) {
			// TODO: if row.Unprocessed() > 0 {
			// TODO: 	panic(fmt.Sprintf("pqlite: %d unprocessed columns", row.Unprocessed()))
			// TODO: }
			return
		}
	}
}

func (st *Stream) next() (mem.RO, error) {
	c := st.c.fc
	for {
		var zero mem.RO
		h, err := c.ReadHeader(st.ctx)
		if err != nil {
			return zero, err
		}
		switch h.Type() {
		case 'H':
			// TODO(bmizerany): track num cols
			if err := c.DiscardMessage(st.ctx); err != nil {
				return zero, err
			}
		case 'd':
			cd, err := c.ReadCopyData(st.ctx)
			if err != nil {
				return zero, err
			}
			return cd.Data, nil
		case 'f':
			panic("TODO: handle copy failure")
		case 'c', '1', '2':
			// CopyDone, BindComplete, ParseComplete: ignore
			continue
		case 'C':
			m, err := c.ReadCommandComplete(st.ctx)
			if err != nil {
				return zero, err
			}
			st.rowsAffected = m.RowsAffected
		case 'Z':
			_, err := c.ReadReadyForQuery(st.ctx)
			if err != nil {
				return zero, err
			}
			return zero, ErrDone
		default:
			err := fmt.Errorf("unexpected message type: %q", h.Type())
			return zero, err
		}
	}
}

func (st *Stream) RowsAffected() int64 {
	return st.rowsAffected
}

func (s *Stream) WriteBytes([]byte) (int, error) {
	panic("TODO")
}

func (s *Stream) ReadFrom(r io.Reader) (int64, error) {
	panic("TODO")
}
