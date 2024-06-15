//go:build ignore

package main

import (
	"context"
	"fmt"
	"log"
)

func main() {
	const dsn = "dbname=postgres user=postgres port=5433 host=localhost sslmode=disable"
	ctx := context.Background()

	cn, err := postgres.Dial(ctx, dsn, nil)
	if err != nil {
		log.Fatal(err)
	}

	dropCN, err := postgres.Dial(ctx, dsn, nil)
	if err != nil {
		log.Fatal(err)
	}

	const query = `
		SELECT datname
		FROM pg_database
		WHERE datname LIKE 'test_%'
	`

	for row, err := range cn.QueryAll(ctx, query) {
		if err != nil {
			log.Fatal(err)
		}
		name, err := row.View()
		if err != nil {
			log.Fatal(err)
		}

		dropQ := fmt.Sprintf("DROP DATABASE %s", name.StringCopy())
		log.Print(dropQ)

		if err := dropCN.Exec(ctx, dropQ); err != nil {
			log.Fatal(err)
		}
	}
}
