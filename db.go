package pqlite

import (
	"context"
	"fmt"
	"net"

	"blake.io/pqlite/febe"
)

type Conn struct {
	c *febe.Conn
}

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

	h, err := c.c.ReadHeader()
	if err != nil {
		return nil, err
	}

	fmt.Printf("header: %v\n", h)

	return c, nil
}

func (c *Conn) Query(sql string) (*Rows, error) {
	return nil, nil
}

type Rows struct {
	c *Conn
}

func (r *Rows) Next() bool {
	return false
}

func (r *Rows) Close() error {
	return nil
}
