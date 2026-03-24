package polite

import (
	"context"
	"fmt"
	"log"
)

func ExampleDial() {
	ctx := context.Background()

	cn, err := Dial(ctx, DefaultDSN, nil)
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

	cn, err := Dial(ctx, DefaultDSN, nil)
	if err != nil {
		return
	}
	defer cn.Close()

	for row := range cn.Query(ctx, "SELECT 1") {
		n, err := row.ParseInt()
		if err != nil {
			return
		}
		fmt.Println(n)
	}
}

func ExamplePool_Get() {
	ctx := context.Background()

	p := new(Pool) // use default settings
	defer p.Close()

	cn, err := p.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Put(cn)

	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		log.Fatal(err)
	}
}
