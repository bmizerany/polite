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

	const q = `
		SELECT id, street, city
		FROM addresses
		ORDER BY id
	`
	for row, err := range cn.Query(ctx, q) {
		if err != nil {
			return
		}
		var address struct {
			ID     int64
			Street string
			City   string
		}
		for i := range row.NumColumns() {
			col := row.Column(i)
			switch col.Name() {
			case "id":
				address.ID, err = col.ParseInt()
				if err != nil {
					return
				}
			case "street":
				address.Street = col.Text()
			case "city":
				address.City = col.Text()
			}
		}
		fmt.Println(address.ID, address.Street, address.City)
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
