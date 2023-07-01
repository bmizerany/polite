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

		fmt.Printf("h %v\n", h)

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
	rs := &Rows{ctx: ctx, c: c}
	return rs, nil
}

type Rows struct {
	ctx context.Context
	c   *Conn
}

func (rs *Rows) Next() error {
	c := rs.c.c
	h, err := c.ReadHeader(rs.ctx)
	if err != nil {
		return err
	}
	switch h.Type() {
	case 'E':
		er, err := c.ReadError(rs.ctx, h)
		if err != nil {
			return err
		}
		return &er.ErrorFields
	default:
		return fmt.Errorf("%w: unknown message type: %q", ErrProtocol, h.Type())
	}
	return nil
}

func (r *Rows) Close() error {
	return nil
}
