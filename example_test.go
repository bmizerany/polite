package polite_test

import (
	"context"
	"fmt"
	"log"

	"blake.io/polite"
)

func ExampleConn_Query_copyTo() {
	ctx := context.Background()
	cn, err := polite.Open(ctx, "postgres://localhost/mydb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	for row, err := range cn.Query(ctx, "COPY foo TO STDOUT WITH (FORMAT binary)") {
		if err != nil {
			log.Fatal(err)
		}
		id, _ := row.ParseInt()
		name, _ := row.ParseString()
		fmt.Println(id, name)
	}
}

func ExampleConn_Query_copyFrom() {
	ctx := context.Background()
	cn, err := polite.Open(ctx, "postgres://localhost/mydb", nil)
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
		if err != nil {
			log.Fatal(err)
		}
		for _, item := range items {
			if err := cn.Exec(ctx, "", item.ID, item.Name); err != nil {
				log.Fatal(err)
			}
		}
	}
}
