package pqlite

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/user"

	"blake.io/pqlite/febe"
)

// Errors
var (
	ErrProtocol = errors.New("pqlite: protocol error")
)

type Conn struct {
	c *febe.Conn
}

func Dial(ctx context.Context, dsnOrURL string) (*Conn, error) {
	o, err := ParseOptions(dsnOrURL)
	if err != nil {
		return nil, err
	}
	if o.User == "" {
		cu, err := user.Current()
		if err != nil {
			return nil, err
		}
		o.User = cu.Username
	}

	var d net.Dialer
	cn, err := d.DialContext(ctx, "tcp", o.Host+":"+o.Port)
	if err != nil {
		return nil, err
	}

	opts := append(o.Extra, "user", o.User)
	c := &Conn{c: febe.New(cn, cn)}
	if err := c.c.WriteStartupMessage(ctx, opts...); err != nil {
		return nil, err
	}

	if err := c.c.Flush(); err != nil {
		return nil, err
	}

	h, err := c.c.ReadHeader(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Printf("h %v\n", h)

	switch h.Type() {
	case 'E':
		er, err := c.c.ReadError(ctx, h)
		if err != nil {
			return nil, err
		}
		return nil, &er
	default:
		return nil, fmt.Errorf("%w: unknown message %q", ErrProtocol, h.Type())
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
