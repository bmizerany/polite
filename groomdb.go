//go:build ignore

package main

import (
	"context"
	"fmt"
	"log"

	"blake.io/pqlite"
)

func main() {
	const dsn = "dbname=postgres user=postgres port=5433 host=localhost sslmode=disable"
	ctx := context.Background()

	// We're running two statements at the same time, so we need two
	// connections. This one is listing the databases. The other one is
	// dropping them.
	cn, err := pqlite.Open(ctx, dsn, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	dropCN, err := pqlite.Open(ctx, dsn, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer dropCN.Close()

	const query = `
		SELECT datname
		FROM pg_database
		WHERE datname LIKE 'test_%'
	`

	for row, err := range cn.Query(ctx, query) {
		if err != nil {
			log.Fatal(err)
		}
		name, err := row.ParseString()
		if err != nil {
			log.Fatal(err)
		}
		dropQ := fmt.Sprintf("DROP DATABASE %s", name)
		log.Print(dropQ)
		if err := dropCN.Exec(ctx, dropQ); err != nil {
			log.Fatal(err)
		}
	}
}
