package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"blake.io/postgres/febe"
	"blake.io/postgres/iter"
)

// TODO: https://www.postgresql.org/docs/current/protocol-flow.html
//
// [x] 55.2.1. Start-up
// [x] 55.2.2. Simple Query
// [x] 55.2.3. Extended Query
// [?] 55.2.5. Function Call
// [ ] 55.2.8. Canceling Requests in Progress
// [ ] 55.2.6. COPY Operations
// [ ] 55.2.7. Asynchronous Operations
//     [x] ParameterStatus
//     [x] Notice
//     [x] Notification
// [x] 55.2.9. Termination
// [ ] 55.2.10. SSL Session Encryption
//
// TODO(bmizerany): QueryOne

// Errors
var (
	ErrProtocol      = errors.New("pqlite: protocol error")
	ErrDone          = errors.New("pqlite: done")
	ErrEmptyQuery    = errors.New("pqlite: empty query")
	ErrNoMoreColumns = errors.New("pqlite: no more columns")
)

type Conn struct {
	dsnOrURL string
	cn       net.Conn
	fc       *febe.Conn
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
	bargs := make([][]byte, len(args))
	for i, arg := range args {
		bargs[i] = []byte(arg)
	}
	extended := len(args) > 0
	if extended {
		if err := c.prepare(ctx, sql, bargs...); err != nil {
			return &Rows{err: err}
		}
	} else {
		if err := c.fc.WriteQuery(ctx, sql); err != nil {
			return &Rows{err: err}
		}
	}
	return &Rows{ctx: ctx, c: c, extended: extended}
}

func (c *Conn) prepare(ctx context.Context, sql string, args ...[]byte) (err error) {
	if err := c.fc.WriteParse(ctx, "", sql); err != nil {
		return err
	}
	return c.fc.WriteBind(ctx, "", "", args)
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

func (r *Row) Reader() (io.ReadCloser, error) {
	if r.rem == 0 {
		return nil, ErrNoMoreColumns
	}
	r.rem--
	rc, err := r.c.fc.ReadDataRowValue(r.ctx)
	if err != nil {
		return nil, err
	}
	return rc, nil
}

func (r *Row) Bytes() ([]byte, error) {
	rc, err := r.Reader()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return data, nil
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
		rc, err := r.Reader()
		if err != nil {
			if errors.Is(err, ErrNoMoreColumns) {
				return nil
			}
			return err
		}
		if err := rc.Close(); err != nil {
			return err
		}
	}
}

type Rows struct {
	ctx context.Context
	c   *Conn
	err error

	extended bool // TODO(bmizerany): replace this

	rowsAffected int64
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
	if rs.extended {
		if err := rs.c.fc.WriteExecute(rs.ctx, "", 0); err != nil {
			yield(nil, err)
			return
		}
		if err := rs.c.fc.WriteSync(rs.ctx); err != nil {
			yield(nil, err)
			return
		}
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

		if !yield(&row, nil) {
			if row.Unprocessed() > 0 {
				panic(fmt.Sprintf("pqlite: %d unprocessed columns", row.Unprocessed()))
			}
			return
		}
	}
}

func (rs *Rows) next() (Row, error) {
	c := rs.c.fc
	for {
		h, err := c.ReadHeader(rs.ctx)
		if err != nil {
			return Row{}, err
		}
		switch h.Type() {
		case '1', '2':
			// Headers only; No discard needed
		case 'C':
			m, err := c.ReadCommandComplete(rs.ctx)
			if err != nil {
				return Row{}, err
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
				return Row{}, err
			}
		case 'D':
			m, err := c.ReadDataRow(rs.ctx)
			if err != nil {
				return Row{}, err
			}
			r := Row{
				ctx: rs.ctx,
				c:   rs.c,
				d:   m,
				rem: int(m.NumCols),
			}
			return r, nil
		case 'I':
			return Row{}, ErrEmptyQuery
		case 'Z':
			_, err := c.ReadReadyForQuery(rs.ctx)
			if err != nil {
				return Row{}, err
			}
			// TODO(bmizerany): save the transaction status
			return Row{}, ErrDone
		default:
			return Row{}, fmt.Errorf("%w: unexpected message type: %q", ErrProtocol, h.Type())
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
