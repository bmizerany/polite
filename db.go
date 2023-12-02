package pqlite

import (
	"context"
	"errors"
	"fmt"
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
	ErrProtocol   = errors.New("pqlite: protocol error")
	ErrComplete   = errors.New("pqlite: complete")
	ErrEmptyQuery = errors.New("pqlite: empty query")
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
		case 'E':
			cr, err := c.c.ReadError(ctx, h)
			if err != nil {
				return nil, err
			}
			return nil, &cr
		case 'R':
			cr, err := c.c.ReadAuthentication(ctx, h)
			if err != nil {
				return nil, err
			}
			if cr.Type != 0 {
				panic("TODO") // support passwrod
			}
		case 'S':
			if err := c.c.DiscardMessage(ctx, h); err != nil {
				return nil, err
			}
		case 'K':
			// TODO(bmizerany): save the key for cancellations later
			if err := c.c.DiscardMessage(ctx, h); err != nil {
				return nil, err
			}
		case 'Z':
			_, err := c.c.ReadReadyForQuery(ctx, h)
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

func (c *Conn) Query(ctx context.Context, sql string) iter_Seq2[Row, error] {
	// TODO(bmizerany): extended query
	return c.simpleQuery(ctx, sql)
}

func (c *Conn) simpleQuery(ctx context.Context, sql string) iter_Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		rows, err := c.QueryRows(ctx, sql)
		if err != nil {
			yield(Row{}, err)
			return
		}
		defer rows.Close()
		for {
			row, err := rows.Next()
			if err != nil {
				if errors.Is(err, ErrComplete) {
					return
				}
				yield(Row{}, err)
				return
			}
			if !yield(row, nil) {
				return
			}
		}
	}
}

type Result struct {
	RowsAffected int
}

// Exec executes a query that is not intended to return rows, such as an
// INSERT, UPDATE, or DELETE. It returns the number of rows affected (not the
// number of rows matched) in a Result or any error encountered.
func (c *Conn) Exec(ctx context.Context, sql string) (Result, error) {
	var zero Result
	rs, err := c.QueryRows(ctx, sql)
	if err != nil {
		return zero, err
	}
	for {
		_, err := rs.Next()
		if err != nil {
			if errors.Is(err, ErrComplete) {
				return rs.Result(), nil
			}
			return rs.Result(), err
		}
	}
}

func (c *Conn) QueryRows(ctx context.Context, sql string) (*Rows, error) {
	if err := c.c.WriteQuery(ctx, sql); err != nil {
		return nil, err
	}
	if err := c.c.Flush(); err != nil {
		return nil, err
	}
	rows := &Rows{ctx: ctx, c: c}
	return rows, nil
}

type Row struct {
	d febe.DataRowResponse
}

func (r *Row) Values() [][]byte {
	return r.d.Values
}

type Rows struct {
	ctx context.Context
	c   *Conn

	rowsAffected int64
}

// RowsAffected returns the number of rows affected by the query. It is only
// valid after Next returns ErrComplete.
func (rs *Rows) Result() Result {
	return Result{RowsAffected: int(rs.rowsAffected)}
}

func (rs *Rows) Next() (Row, error) {
	c := rs.c.c
	for {
		h, err := c.ReadHeader(rs.ctx)
		if err != nil {
			return Row{}, err
		}
		switch h.Type() {
		case 'C':
			m, err := c.ReadCommandComplete(rs.ctx, h)
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
			if err := c.DiscardMessage(rs.ctx, h); err != nil {
				return Row{}, err
			}
		case 'D':
			m, err := c.ReadDataRow(rs.ctx, h)
			if err != nil {
				return Row{}, err
			}
			return Row{d: m}, nil
		case 'I':
			return Row{}, ErrEmptyQuery
		case 'E':
			er, err := c.ReadError(rs.ctx, h)
			if err != nil {
				return Row{}, err
			}
			return Row{}, &er.ErrorFields
		case 'Z':
			_, err := c.ReadReadyForQuery(rs.ctx, h)
			if err != nil {
				return Row{}, err
			}
			// TODO(bmizerany): save the transaction status
			return Row{}, ErrComplete
		case 'N':
			panic("TODO: handle notice")
		default:
			return Row{}, fmt.Errorf("%w: unknown message type: %q", ErrProtocol, h.Type())
		}
	}
}

func (r *Rows) Close() error {
	return nil
}
