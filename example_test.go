package polite_test

import (
	"context"
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
		var id int
		var name string
		for col := range row.Columns() {
			switch col.Index() {
			case 0:
				id, err = col.ParseInt()
			case 1:
				name, err = col.ParseString()
			}
			if err != nil {
				log.Fatal(err)
			}
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
