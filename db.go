package pqlite

import (
	"context"
	"errors"
	"fmt"
	"net"

	"blake.io/pqlite/febe"
)

// Errors
var (
	ErrProtocol = errors.New("pqlite: protocol error")
	ErrComplete = errors.New("pqlite: complete")
)

type Conn struct {
	c *febe.Conn
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

	c := &Conn{c: febe.New(cn, cn)}
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

func (c *Conn) Query(ctx context.Context, sql string) (*Rows, error) {
	if err := c.c.WriteQuery(ctx, sql); err != nil {
		return nil, err
	}
	if err := c.c.Flush(); err != nil {
		return nil, err
	}
	rs := &Rows{ctx: ctx, c: c}
	return rs, nil
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
}

func (rs *Rows) Next() (Row, error) {
	c := rs.c.c
	for {
		h, err := c.ReadHeader(rs.ctx)
		if err != nil {
			return Row{}, err
		}
		switch h.Type() {
		case 'E':
			er, err := c.ReadError(rs.ctx, h)
			if err != nil {
				return Row{}, err
			}
			return Row{}, &er.ErrorFields
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
		case 'C':
			_, err := c.ReadCommandComplete(rs.ctx, h)
			if err != nil {
				return Row{}, err
			}
			// TODO(bmizerany): save the command tag
		case 'Z':
			_, err := c.ReadReadyForQuery(rs.ctx, h)
			if err != nil {
				return Row{}, err
			}
			// TODO(bmizerany): save the transaction status
			return Row{}, ErrComplete
		default:
			return Row{}, fmt.Errorf("%w: unknown message type: %q", ErrProtocol, h.Type())
		}
	}
}

func (r *Rows) Close() error {
	return nil
}
