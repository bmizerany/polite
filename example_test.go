package polite_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"blake.io/polite"
)

func ExampleConn_Query_copyTo() {
	ctx := context.Background()
	cn, err := polite.Dial(ctx, "postgres://localhost/mydb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	for row, err := range cn.Query(ctx, "COPY foo TO STDOUT WITH (FORMAT binary)") {
		if err != nil {
			log.Fatal(err)
		}
		id, err := row.Column(0).DecodeInt()
		if err != nil {
			log.Fatal(err)
		}
		name := row.Column(1).Text()
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
	for row, err := range cn.Query(ctx, q) {
		if err != nil {
			log.Fatal(err)
		}

		var user User
		err := json.Unmarshal(row.Column(0).Bytes(), &user)
		if err != nil {
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
