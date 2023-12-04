package pqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"blake.io/pqlite/febe"
)

// TODO: https://www.postgresql.org/docs/current/protocol-flow.html
//
// [x] 55.2.1. Start-up
// [ ]    -- tls
// [x] 55.2.2. Simple Query
// [ ] 55.2.3. Extended Query
// [/] 55.2.4. Pipelining (not supporting)
// [?] 55.2.5. Function Call
// [ ] 55.2.6. COPY Operations
// [ ] 55.2.7. Asynchronous Operations
// [ ] 55.2.8. Canceling Requests in Progress
// [ ] 55.2.9. Termination
// [ ] 55.2.10. SSL Session Encryption
// [/] 55.2.11. GSSAPI Session Encryption

// Errors
var (
	ErrProtocol      = errors.New("pqlite: protocol error")
	ErrReadyForQuery = errors.New("pqlite: complete")
	ErrEmptyQuery    = errors.New("pqlite: empty query")
	ErrNoMoreColumns = errors.New("pqlite: no more columns")
)

type Conn struct {
	dsnOrURL string
	cn       net.Conn
	c        *febe.Conn
}

// Dial connects to postgres using the parameters in dsnOrURL, which is a DSN
// or URL. The host, port, and sslmode parameters are defaulted to localhost,
// 5432, and verirfy, respectivly. No other parameter has a default, including
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
		c:        febe.New(cn, cn),
	}
	if err := c.c.WriteStartupMessage(ctx, o.Extra...); err != nil {
		return nil, err
	}

	if err := c.c.Flush(); err != nil {
		return nil, err
	}

	for {
		h, err := c.c.ReadHeader(ctx)
		if err != nil {
			return nil, err
		}

		switch h.Type() {
		case 'R':
			cr, err := c.c.ReadAuthentication(ctx)
			if err != nil {
				return nil, err
			}
			if cr.Type != 0 {
				panic("TODO") // support passwrod
			}
		case 'S':
			if err := c.c.DiscardMessage(ctx); err != nil {
				return nil, err
			}
		case 'K':
			// TODO(bmizerany): save the key for cancellations later
			if err := c.c.DiscardMessage(ctx); err != nil {
				return nil, err
			}
		case 'Z':
			_, err := c.c.ReadReadyForQuery(ctx)
			if err != nil {
				return nil, err
			}
			return c, nil
		default:
			return nil, fmt.Errorf("%w: unknown message %q", ErrProtocol, h.Type())
		}
	}
}

func (c *Conn) Close() error {
	return c.cn.Close()
}

// TODO(bmizerany): replace with iter.Seq2 in go1.22
type iter_Seq2[T, U any] func(yield func(T, U) bool)

type Result struct {
	RowsAffected int
}

// Exec executes a query that is not intended to return rows, such as an
// INSERT, UPDATE, or DELETE. It returns the number of rows affected (not the
// number of rows matched) in a Result or any error encountered.
func (c *Conn) Exec(ctx context.Context, sql string) (Result, error) {
	rs := c.Query(ctx, sql)
	err := rs.Close()
	return rs.Result(), err
}

func (c *Conn) Query(ctx context.Context, sql string) *Rows {
	if err := c.c.WriteQuery(ctx, sql); err != nil {
		return &Rows{err: err}
	}
	if err := c.c.Flush(); err != nil {
		return &Rows{err: err}
	}
	return &Rows{ctx: ctx, c: c}
}

// QueryAll is short for:
//
//	rs := c.Query(ctx, sql)
//	defer rs.Close()
//	for row, err := range rs.All {
//	  ...
//	}
//
// Example:
//
//	for row, err := range c.QueryAll(ctx, "SELECT * FROM users") {
//	  ...
//	}
func (c *Conn) QueryAll(ctx context.Context, sql string) iter_Seq2[*Row, error] {
	return func(yield func(*Row, error) bool) {
		rs := c.Query(ctx, sql)
		defer rs.Close()
		rs.All(yield)
	}
}

type Row struct {
	ctx context.Context
	c   *Conn
	d   febe.DataRowResponse
	rem int
}

// NumCols returns the number of columns in a row.
func (r *Row) NumCols() int {
	return int(r.d.NumCols)
}

// NumUnprocessed returns the number of columns that have not been
// processed.
func (r *Row) NumUnprocessed() int {
	return r.rem
}

func (r *Row) Reader() (io.ReadCloser, error) {
	if r.rem == 0 {
		return nil, ErrNoMoreColumns
	}
	r.rem--
	rc, err := r.c.c.ReadDataRowValue(r.ctx)
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
	for {
		row, err := rs.next()
		if err != nil {
			rs.err = err
			if !errors.Is(err, ErrReadyForQuery) {
				yield(nil, err)
			}
			return
		}

		func() {
			defer row.Close()
			if !yield(&row, nil) {
				if row.NumUnprocessed() > 0 {
					err := fmt.Errorf("pqlite: %d columns unprocessed", row.rem)
					yield(nil, err)
				}
				return
			}
		}()
	}
}

func (rs *Rows) next() (Row, error) {
	c := rs.c.c
	for {
		h, err := c.ReadHeader(rs.ctx)
		if err != nil {
			return Row{}, err
		}
		switch h.Type() {
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
			// espcially when code can be generated from the
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
			return Row{}, ErrReadyForQuery
		case 'N':
			panic("TODO: handle notice")
		default:
			return Row{}, fmt.Errorf("%w: unknown message type: %q", ErrProtocol, h.Type())
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
