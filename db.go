package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"blake.io/postgres/febe"
	"blake.io/postgres/iter"
	"go4.org/mem"
)

// TODO: https://www.postgresql.org/docs/current/protocol-flow.html
//
// [ ] 55.2.8. Canceling Requests in Progress
// [ ] 55.2.6. COPY Operations
// [ ] 55.2.10. SSL Session Encryption
//
// TODO(bmizerany): QueryOne

// Errors
var (
	ErrProtocol     = errors.New("pqlite: protocol error")
	ErrDone         = errors.New("pqlite: done")
	ErrEmptyQuery   = errors.New("pqlite: empty query")
	ErrOutOfColumns = errors.New("pqlite: out of columns")
)

type statement struct {
	name string
	febe.RowDescriptionResponse
}

type Conn struct {
	dsnOrURL string
	cn       io.Closer
	fc       *febe.Conn

	nstmt int
	cache map[string]*statement

	rows         *Rows
	rowsAffected int64
}

func Ping(ctx context.Context, cn *Conn) error {
	return cn.Exec(ctx, "SELECT 1")
}

func (c *Conn) Close() error {
	return c.cn.Close()
}

type Result struct {
	RowsAffected int
}

// Exec executes a query that is not intended to return rows, such as an
// INSERT, UPDATE, or DELETE. It returns the number of rows affected (not the
// number of rows matched) in a [Result] or any error encountered.
func (c *Conn) Exec(ctx context.Context, sql string, args ...string) error {
	return c.Query(ctx, sql, args...).Close()
}

func (c *Conn) LastResult() Result {
	if c.rows == nil {
		return Result{}
	}
	return c.rows.Result()
}

func (c *Conn) Query(ctx context.Context, sql string, args ...string) *Rows {
	if err := c.prepare(ctx, "", sql, args...); err != nil {
		return &Rows{err: err}
	}
	if c.rows == nil {
		c.rows = &Rows{}
	}
	*c.rows = Rows{ctx: ctx, c: c, row: c.rows.row}
	return c.rows
}

func (c *Conn) prepare(ctx context.Context, portal, sql string, args ...string) (err error) {
	stmt, err := c.getStmt(ctx, sql)
	if err != nil {
		return err
	}
	return c.fc.WriteBind(ctx, portal, stmt.name, args)
}

func (c *Conn) newStmtName() (string, error) {
	stmt := strconv.Itoa(c.nstmt)
	c.nstmt++
	return stmt, nil
}

func (c *Conn) getStmt(ctx context.Context, sql string) (*statement, error) {
	if c.cache == nil {
		c.cache = make(map[string]*statement)
	}
	stmt := c.cache[sql]
	if stmt != nil {
		return stmt, nil
	}
	name, err := c.newStmtName()
	if err != nil {
		return nil, err
	}
	if err := c.fc.WriteParse(ctx, name, sql); err != nil {
		return nil, err
	}
	if err := c.fc.WriteDescribe(ctx, 'S', name); err != nil {
		return nil, err
	}

	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return nil, err
		}
		switch h.Type() {
		case '1':
			continue
		case 'T':
			rd, err := c.fc.ReadRowDescription(ctx)
			if err != nil {
				return nil, err
			}
			c.cache[sql] = &statement{
				name:                   name,
				RowDescriptionResponse: rd,
			}
			return stmt, nil
		default:
			return nil, fmt.Errorf("unexpected message type: %q", h.Type())
		}
	}
}

// QueryAll is short for:
//
//	rs := c.Query(ctx, sql)
//	defer rs.Close()
//	for row, err := range rs.All {
//	  ...
//	}
//
// Callers must process all columns in a row. If a call to yield does not
// process all columns in the provided rows before returning, QueryAll panics.
//
// If a caller needs to discard any remaining rows, it should call rs.Close
// before returning from yield.
//
// Example:
//
//	for row, err := range c.QueryAll(ctx, "SELECT * FROM users") {
//	  ...
//	}
func (c *Conn) QueryAll(ctx context.Context, sql string, args ...string) iter.Seq2[*Row, error] {
	return func(yield func(*Row, error) bool) {
		rs := c.Query(ctx, sql, args...)
		defer rs.Close()
		for row, err := range rs.All {
			if !yield(row, err) {
				return
			}
		}
	}
}

func (c *Conn) execAndSync(ctx context.Context, portal string) error {
	c.fc.WriteExecute(ctx, "", 0)
	c.fc.WriteSync(ctx)
	if err := c.fc.Flush(); err != nil {
		return err
	}
	return nil
}

type Row struct {
	ctx     context.Context
	c       *Conn
	numCols int
	rem     int
}

// Columns returns the number of columns in a row.
func (r *Row) Columns() int {
	return int(r.numCols)
}

// Unprocessed returns the number of columns that have not been
// processed.
func (r *Row) Unprocessed() int {
	return r.rem
}

func (r *Row) View() (mem.RO, error) {
	if r.rem == 0 {
		return mem.RO{}, ErrOutOfColumns
	}
	r.rem--
	v, err := r.c.fc.ReadDataRowValue(r.ctx)
	if err != nil {
		return mem.RO{}, err
	}
	return mem.B(v.Data), nil
}

// Close discards any remaining columns in the row.
//
// Most users will not need to call this method, since Rows.All will call it
// automatically after each yield.
func (r *Row) Close() error {
	if r.rem == 0 {
		// nothing to discard; carry on
		return nil
	}
	for {
		_, err := r.View()
		if err != nil {
			if errors.Is(err, ErrOutOfColumns) {
				return nil
			}
			return err
		}
	}
}

type Rows struct {
	ctx context.Context
	c   *Conn
	err error

	rowsAffected int64

	row *Row // owned by next()
}

// RowsAffected returns the number of rows affected by the query. It is only
// valid after all rows have been read.
func (rs *Rows) Result() Result {
	return Result{RowsAffected: int(rs.rowsAffected)}
}

func (rs *Rows) All(yield func(*Row, error) bool) {
	if rs.err != nil {
		yield(nil, rs.err)
		return
	}
	if err := rs.c.execAndSync(rs.ctx, ""); err != nil {
		yield(nil, err)
		return
	}
	for {
		row, err := rs.next()
		if err != nil {
			rs.err = err
			if errors.Is(err, ErrDone) {
				return
			}
			yield(nil, err)
			return
		}

		ok := yield(row, nil)
		if row.Unprocessed() > 0 {
			panic(fmt.Sprintf("pqlite: %d unprocessed columns", row.Unprocessed()))
		}
		if !ok {
			return
		}
	}
}

func (rs *Rows) next() (*Row, error) {
	c := rs.c.fc
	for {
		h, err := c.ReadHeader(rs.ctx)
		if err != nil {
			return nil, err
		}
		switch h.Type() {
		case '2':
			// Headers only; No discard needed
			continue
		case 'C':
			m, err := c.ReadCommandComplete(rs.ctx)
			if err != nil {
				return nil, err
			}
			rs.rowsAffected = m.RowsAffected
		case 'D':
			m, err := c.ReadDataRow(rs.ctx)
			if err != nil {
				return nil, err
			}
			if rs.row == nil {
				rs.row = &Row{}
			}
			*rs.row = Row{
				ctx:     rs.ctx,
				c:       rs.c,
				numCols: int(m.NumCols),
				rem:     int(m.NumCols),
			}
			return rs.row, nil
		case 'I':
			return nil, ErrEmptyQuery
		case 'Z':
			_, err := c.ReadReadyForQuery(rs.ctx)
			if err != nil {
				return nil, err
			}
			// TODO(bmizerany): save the transaction status
			return nil, ErrDone
		default:
			return nil, fmt.Errorf("%w: Rows.next: unexpected message type: %q", ErrProtocol, h.Type())
		}
	}
}

func (rs *Rows) Close() error {
	for row, err := range rs.All {
		if err != nil {
			return err
		}
		if err := row.Close(); err != nil {
			return err
		}
	}
	return nil
}
