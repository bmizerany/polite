//go:build ignore

package main

import (
	"context"
	"fmt"
	"log"

	"blake.io/postgres"
)

func main() {
	const dsn = "dbname=postgres user=postgres"
	ctx := context.Background()

	cn, err := postgres.Dial(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}

	dropCN, err := postgres.Dial(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}

	const query = `
		SELECT datname
		FROM pg_database
		WHERE datname LIKE 'test\_%'
	`

	for row, err := range cn.QueryAll(ctx, query) {
		if err != nil {
			log.Fatal(err)
		}
		name, err := row.Bytes()
		if err != nil {
			log.Fatal(err)
		}

		dropQ := fmt.Sprintf("DROP DATABASE %s", name)
		log.Print(dropQ)

		_, err = dropCN.Exec(ctx, dropQ)
		if err != nil {
			log.Fatal(err)
		}
	}
}
