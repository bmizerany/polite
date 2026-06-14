// The examples in this file elide error handling for brevity. Real
// code must check every returned error.

package polite_test

import (
	"context"
	"encoding/json"
	"fmt"

	"blake.io/polite"
)

// Dial opens a connection using the default DSN, runs a single command, and
// closes. The shape every other example builds on.
func ExampleDial() {
	ctx := context.Background()

	cn, _ := polite.Dial(ctx, "", nil)
	defer cn.Close()

	cn.Exec(ctx, "SELECT 1")
}

// Ranging over Columns and switching on Name reads each value by
// the name it has in the row. Pick this when the column positions
// in the query might shift, or when the query is SELECT * against
// a table whose column list can grow. Reordering the SELECT, or
// adding a column to the table, does not miswire fields here.
func ExampleRow_Columns() {
	ctx := context.Background()

	cn, _ := polite.Dial(ctx, "", nil)
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
				address.ID, _ = col.DecodeInt()
			case "street":
				address.Street, _ = col.DecodeText()
			case "city":
				address.City, _ = col.DecodeText()
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
// when the schema is wide or changes often. Use the Row.Columns
// example when you want to avoid the encode/decode.
func ExampleRow_Column_toStruct() {
	ctx := context.Background()

	cn, _ := polite.Dial(ctx, "", nil)
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
		b, _ := row.Column(0).Bytes()
		var user User
		json.Unmarshal(b, &user)
		fmt.Printf("%#v\n", user)
	}
}

// Row.Column(i) addresses a column by position. It fits a tight
// query with a small, fixed column list, and it is also the way to
// read columns out of order or more than once without reshaping the
// SELECT. The column order in the query is load-bearing: swap two
// columns in the SELECT and the decode silently reads the wrong
// field. SELECT * is a bad fit here for the same reason; see the
// Row.Columns example or the struct variant of this example.
func ExampleRow_Column() {
	ctx := context.Background()

	cn, _ := polite.Dial(ctx, "", nil)
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
		address.ID, _ = row.Column(0).DecodeInt()
		address.Street, _ = row.Column(1).DecodeText()
		address.City, _ = row.Column(2).DecodeText()
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
	cn, _ := polite.Dial(ctx, "postgres://localhost/mydb", nil)
	defer cn.Close()

	var (
		id   int64
		name string
	)
	for row := range cn.Query(ctx, "COPY foo TO STDOUT WITH (FORMAT binary)") {
		id, _ = row.Column(0).DecodeInt()
		name, _ = row.Column(1).DecodeText()
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
	cn, _ := polite.Dial(ctx, "postgres://localhost/mydb", nil)
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
			cn.Exec(ctx, "", item.ID, item.Name)
		}
	}
}

// LISTEN on a channel and range over Notifications. The range ends only
// when the caller breaks or the connection becomes terminal; attach a
// febe.Trace if you need to tell the two apart.
func ExampleNotifications_listenNotify() {
	ctx := context.Background()

	cn, _ := polite.Dial(ctx, "postgres://localhost/mydb", nil)
	defer cn.Close()

	cn.Exec(ctx, "LISTEN chat")

	for n := range polite.Notifications(ctx, cn) {
		fmt.Printf("channel=%s payload=%s\n", n.Channel, n.Append(nil))
	}
}

// SQL casts let the placeholder's encoded type differ from the
// column's. Polite tells PostgreSQL each parameter's type up front,
// so a cast in the SQL is a server-side coercion rather than a
// workaround for the wire format.
//
// Pass a Go int into a text column with $1::text. Pass a Go string
// into an int4 column with $1::int4. The same pattern reaches types
// polite does not encode directly — uuid, inet, numeric, jsonb —
// because PostgreSQL accepts a text representation for each.
func ExampleConn_Query_sqlCast() {
	ctx := context.Background()
	cn, _ := polite.Dial(ctx, "", nil)
	defer cn.Close()

	// Go int -> text.
	for row := range cn.Query(ctx, `SELECT $1::text`, 42) {
		s, _ := row.Column(0).DecodeText()
		fmt.Println(s)
	}

	// Go string -> int4.
	for row := range cn.Query(ctx, `SELECT $1::int4`, "42") {
		n, _ := row.Column(0).DecodeInt()
		fmt.Println(n)
	}
}

// NULL columns are detected by the contract on Column.Bytes: a SQL
// NULL returns a nil slice with a nil error; a non-NULL value
// returns a non-nil slice. The error must be checked first; only
// then does b == nil mean NULL.
//
// This example handles the error explicitly because the error is
// what distinguishes NULL from "could not read."
func ExampleColumn_Bytes_nullCheck() {
	ctx := context.Background()
	cn, _ := polite.Dial(ctx, "", nil)
	defer cn.Close()

	for row := range cn.Query(ctx, "SELECT n FROM t WHERE id = $1", 42) {
		col := row.Column(0)
		b, err := col.Bytes()
		if err != nil {
			panic(err)
		}
		if b == nil {
			fmt.Println("NULL")
			continue
		}
		n, _ := col.DecodeInt() // bytes confirmed non-nil
		fmt.Println(n)
	}
}

func ExamplePool_Get() {
	ctx := context.Background()

	p := new(polite.Pool) // use default settings
	defer p.Close()

	cn, _ := p.Get(ctx)
	defer p.Put(cn)

	cn.Exec(ctx, "SELECT 1")
}
