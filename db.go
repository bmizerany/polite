package pqlite

import (
	"blake.io/pqlite/febe"
)

type Conn struct {
	c *febe.Conn
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
