package polite_test

import (
	"context"
	json "encoding/json/v2"
	"errors"
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
		var id int64
		var name string
		col := row.Column(0)
		id, err = col.ParseInt()
		if err != nil {
			log.Fatal(err)
		}
		col = row.Column(1)
		name = col.Text()
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

	for _, err := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		if !errors.Is(err, polite.ErrCopyFrom) {
			log.Fatal(err)
		}
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

	for row, err := range cn.Query(ctx, `
		SELECT row_to_json(t)
		FROM (
			SELECT 1::int4 AS id, 'alice'::text AS name
			UNION ALL
			SELECT 2::int4, 'bob'::text
		) AS t
	`) {
		if err != nil {
			log.Fatal(err)
		}

		col := row.Column(0)
		var user User
		if err := json.Unmarshal(col.Bytes(), &user); err != nil {
			log.Fatal(err)
		}
		_ = user
	}
}
