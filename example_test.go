package polite_test

import (
	"context"
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

	for row := range cn.Query(ctx, "COPY foo TO STDOUT WITH (FORMAT binary)") {
		id, err := row.ParseInt()
		if err != nil {
			log.Fatal(err)
		}
		name, err := row.ParseString()
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

	for row := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
		_, err := row.View()
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
