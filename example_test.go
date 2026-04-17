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

// Ranging over Columns and switching on Name reads each value by
// the name it has in the row. Pick this when the column positions
// in the query might shift, or when the query is SELECT * against
// a table whose column list can grow. Reordering the SELECT, or
// adding a column to the table, does not miswire fields here.
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

// This is the columns-by-name example in a different shape. The work
// is the same: pull values out of a row and put them into a struct.
// Here we let the server assemble the row as JSON and let
// encoding/json do the field dispatch, so the client code does not
// spell out a switch over column names. It is a convenience, not a
// feature. The cost is a JSON round-trip; the benefit is less code
// when the schema is wide or changes often. Use the ColumnsByName
// example when you want to avoid the encode/decode.
func ExampleConn_Query_columnsToStruct() {
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

// Row.Column(i) addresses a column by position. It fits a tight
// query with a small, fixed column list, and it is also the way to
// read columns out of order or more than once without reshaping the
// SELECT. The column order in the query is load-bearing: swap two
// columns in the SELECT and the decode silently reads the wrong
// field. SELECT * is a bad fit here for the same reason; see the
// ColumnsByName or ColumnsToStruct examples.
func ExampleConn_Query_columnsByIndex() {
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
		address.ID, err = row.Column(0).DecodeInt()
		if err != nil {
			return
		}
		address.Street, err = row.Column(1).Text()
		if err != nil {
			return
		}
		address.City, err = row.Column(2).Text()
		if err != nil {
			return
		}
		fmt.Printf("%#v\n", address)
	}
}

// COPY ... TO STDOUT streams rows out of the server without going
// back through the executor for each row, which is the fastest way
// to read a lot of data. The binary format skips text conversion on
// both sides. Columns have no names in the COPY stream, so access
// is by index.
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

// COPY ... FROM STDIN is the inverse: bulk-load rows into a table
// without a round-trip per row. Query starts the COPY and the loop
// body sends each tuple with Exec and an empty SQL string, which
// the driver recognizes as a COPY data message. The outer range
// runs once; the row value is unused.
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

func ExampleNotifications() {
	ctx := context.Background()

	cn, err := polite.Dial(ctx, "postgres://localhost/mydb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	if err := cn.Exec(ctx, "LISTEN chat"); err != nil {
		log.Fatal(err)
	}

	for n := range polite.Notifications(ctx, cn) {
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
