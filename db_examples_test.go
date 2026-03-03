package pqlite

import (
	"context"
	"fmt"
)

func ExampleOpen() {
	ctx := context.Background()

	cn, err := Open(ctx, DefaultDSN, nil)
	if err != nil {
		return
	}
	defer cn.Close()

	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		return
	}

	_ = cn.LastResult()
}

func ExampleConn_Query() {
	ctx := context.Background()

	cn, err := Open(ctx, DefaultDSN, nil)
	if err != nil {
		return
	}
	defer cn.Close()

	for row, err := range cn.Query(ctx, "SELECT 1") {
		if err != nil {
			return
		}
		n, err := row.ParseInt()
		if err != nil {
			return
		}
		fmt.Println(n)
	}
}
