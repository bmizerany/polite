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
}

func ExampleConn_Query() {
	ctx := context.Background()

	cn, err := Dial(ctx, DefaultDSN, nil)
	if err != nil {
		return
	}
	defer cn.Close()

	const q = `
		SELECT id, street, city
		FROM addresses
		ORDER BY id
	`
	for row := range cn.Query(ctx, q) {
		var address struct {
			ID     int64
			Street string
			City   string
		}
		for col := range row.Columns() {
			switch col.Name() {
			case "id":
				address.ID, err = col.DecodeInt()
				if err != nil {
					return
				}
			case "street":
				address.Street = col.Text()
			case "city":
				address.City = col.Text()
			}
		}
		fmt.Printf("%#v\n", address)
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
