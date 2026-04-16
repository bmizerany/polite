package polite_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"blake.io/polite"
)

func ExampleDial() {
	ctx := context.Background()

	cn, err := polite.Dial(ctx, polite.DefaultDSN, nil)
	if err != nil {
		return
	}
	defer cn.Close()

	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		return
	}
}

func ExampleConn_Query_columnsByName() {
	ctx := context.Background()

	cn, err := polite.Dial(ctx, polite.DefaultDSN, nil)
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
				address.Street, err = col.Text()
				if err != nil {
					return
				}
			case "city":
				address.City, err = col.Text()
				if err != nil {
					return
				}
			}
		}
		fmt.Printf("%#v\n", address)
	}
}

func ExampleConn_Query_copyTo() {
	ctx := context.Background()
	cn, err := polite.Dial(ctx, "postgres://localhost/mydb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	for row := range cn.Query(ctx, "COPY foo TO STDOUT WITH (FORMAT binary)") {
		id, err := row.Column(0).DecodeInt()
		if err != nil {
			log.Fatal(err)
		}
		name, err := row.Column(1).Text()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name)
	}
}

func ExampleConn_Query_copyFrom() {
	ctx := context.Background()
	cn, err := polite.Dial(ctx, "postgres://localhost/mydb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	items := []struct {
		ID   int32
		Name string
	}{
		{1, "alice"},
		{2, "bob"},
	}

	for range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		for _, item := range items {
			if err := cn.Exec(ctx, "", item.ID, item.Name); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func ExampleConn_Query_struct() {
	ctx := context.Background()

	cn, err := polite.Dial(ctx, "", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	type User struct {
		ID   int32  `json:"id"`
		Name string `json:"name"`
	}

	const q = `
		SELECT row_to_json(t)
		FROM (
			SELECT 1::int4 AS id, 'alice'::text AS name
			UNION ALL
			SELECT 2::int4, 'bob'::text
		) AS t
	`
	for row := range cn.Query(ctx, q) {
		b, err := row.Column(0).Bytes()
		if err != nil {
			log.Fatal(err)
		}
		var user User
		if err := json.Unmarshal(b, &user); err != nil {
			log.Fatal(err)
		}
		_ = user
	}
}

func ExampleConn_listenNotify() {
	ctx := context.Background()

	cn, err := polite.Dial(ctx, "postgres://localhost/mydb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	if err := cn.Exec(ctx, "LISTEN chat"); err != nil {
		log.Fatal(err)
	}

	for n := range polite.Events(ctx, cn) {
		fmt.Printf("channel=%s payload=%s\n", n.Channel, n.Payload)
	}
}

func ExamplePool_Get() {
	ctx := context.Background()

	p := new(polite.Pool) // use default settings
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
