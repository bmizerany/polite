package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
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

type Conn struct {
	dsnOrURL string
	cn       io.Closer
	fc       *febe.Conn

	nstmt int
	cache map[string]string // sql -> stmt name

	args []mem.RO

	rows *Rows
}

// Dial connects to postgres using the parameters in dsnOrURL, which is a DSN
// or URL. The host, port, and sslmode parameters are defaulted to localhost,
// 5432, and verify, respectively. No other parameter has a default, including
// user, which must be specified by users of Dial.
func Dial(ctx context.Context, dsnOrURL string) (*Conn, error) {
	o, err := ParseOptions(dsnOrURL)
	if err != nil {
		return nil, err
	}

	var d net.Dialer
	cn, err := d.DialContext(ctx, "tcp", o.Host+":"+o.Port)
	if err != nil {
		return nil, err
	}

	c := &Conn{
		dsnOrURL: dsnOrURL,
		cn:       cn,
		fc:       febe.New(cn, cn),
	}
	if err := c.fc.WriteStartupMessage(ctx, o.Extra...); err != nil {
		return nil, err
	}

	if err := c.fc.Flush(); err != nil {
		return nil, err
	}

	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return nil, err
		}

		switch h.Type() {
		case 'R':
			cr, err := c.fc.ReadAuthentication(ctx)
			if err != nil {
				return nil, err
			}
			if cr.Type != 0 {
				panic("TODO") // support password
			}
		case 'S':
			if err := c.fc.DiscardMessage(ctx); err != nil {
				return nil, err
			}
		case 'K':
			// TODO(bmizerany): save the key for cancellations later
			if err := c.fc.DiscardMessage(ctx); err != nil {
				return nil, err
			}
		case 'Z':
			_, err := c.fc.ReadReadyForQuery(ctx)
			if err != nil {
				return nil, err
			}
			return c, nil
		default:
			return nil, fmt.Errorf("%w: unexpected message %q", ErrProtocol, h.Type())
		}
	}
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
func (c *Conn) Exec(ctx context.Context, sql string, args ...string) (Result, error) {
	rs := c.Query(ctx, sql, args...)

	// This is subtle: Result is not set until after all rows are read.
	// This means `return rs.Result(), rs.Close()` will always return an
	// empty Result because the Close is called after rs.Result().
	err := rs.Close()
	return rs.Result(), err
}

func (c *Conn) Query(ctx context.Context, sql string, args ...string) *Rows {
	c.args = slices.Grow(c.args[:0], min(len(args), 100))
	for _, arg := range args {
		c.args = append(c.args, mem.S(arg))
	}
	if err := c.prepare(ctx, sql, c.args...); err != nil {
		return &Rows{err: err}
	}
	if c.rows == nil {
		c.rows = &Rows{}
	}
	*c.rows = Rows{ctx: ctx, c: c, row: c.rows.row}
	return c.rows
}

func (c *Conn) prepare(ctx context.Context, sql string, args ...mem.RO) (err error) {
	stmt, err := c.getStmt(ctx, sql)
	if err != nil {
		return err
	}
	return c.fc.WriteBind(ctx, "", stmt, args)
}

func (c *Conn) newStmtName() (string, error) {
	stmt := strconv.Itoa(c.nstmt)
	c.cache[stmt] = stmt
	c.nstmt++
	return stmt, nil
}

func (c *Conn) getStmt(ctx context.Context, sql string) (string, error) {
	if c.cache == nil {
		c.cache = make(map[string]string)
	}
	stmt := c.cache[sql]
	if stmt != "" {
		return stmt, nil
	}
	stmt, err := c.newStmtName()
	if err != nil {
		return "", err
	}
	if err := c.fc.WriteParse(ctx, stmt, sql); err != nil {
		return "", err
	}
	c.cache[sql] = stmt
	return stmt, nil
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

type Row struct {
	ctx context.Context
	c   *Conn
	d   febe.DataRowResponse
	rem int
}

// Columns returns the number of columns in a row.
func (r *Row) Columns() int {
	return int(r.d.NumCols)
}

// Unprocessed returns the number of columns that have not been
// processed.
func (r *Row) Unprocessed() int {
	return r.rem
}

func (r *Row) Bytes() (mem.RO, error) {
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
	for range r.rem {
		_, err := r.c.fc.ReadDataRowValue(r.ctx)
		if err != nil {
			return err
		}
		r.rem--
	}
	return nil
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
	if err := rs.c.fc.WriteExecute(rs.ctx, "", 0); err != nil {
		yield(nil, err)
		return
	}
	if err := rs.c.fc.WriteSync(rs.ctx); err != nil {
		yield(nil, err)
		return
	}
	if err := rs.c.fc.Flush(); err != nil {
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

		if !yield(row, nil) {
			if row.Unprocessed() > 0 {
				panic(fmt.Sprintf("pqlite: %d unprocessed columns", row.Unprocessed()))
			}
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
		case '1', '2':
			// Headers only; No discard needed
		case 'C':
			m, err := c.ReadCommandComplete(rs.ctx)
			if err != nil {
				return nil, err
			}
			rs.rowsAffected = m.RowsAffected
		case 'G':
			panic("TODO: CopyInResponse")
		case 'H':
			panic("TODO: CopyOutResponse")
		case 'T':
			// Ignore row descriptions for now; they're pretty
			// useless if you know the query you're running,
			// especially when code can be generated from the
			// query, which would already know the types. Save
			// the energy.
			if err := c.DiscardMessage(rs.ctx); err != nil {
				return nil, err
			}
		case 'D':
			m, err := c.ReadDataRow(rs.ctx)
			if err != nil {
				return nil, err
			}
			if rs.row == nil {
				rs.row = &Row{}
			}
			*rs.row = Row{
				ctx: rs.ctx,
				c:   rs.c,
				d:   m,
				rem: int(m.NumCols),
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
			return nil, fmt.Errorf("%w: unexpected message type: %q", ErrProtocol, h.Type())
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
