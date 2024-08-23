//go:build ignore

package main

import (
	"context"
	"fmt"
	"time"

	"blake.io/pqlite"
	"github.com/jackc/pgx/v5"
)

func main() {
	fmt.Println("PGX (hit any key to continue)")
	fmt.Scanln()
	start := time.Now()
	doPGX()
	fmt.Println(time.Since(start))

	fmt.Println("PQLite (hit any key to continue)")
	fmt.Scanln()
	start = time.Now()
	doPqlite()
	fmt.Println(time.Since(start))

}

func doPGX() {
	ctx := context.Background()
	cn, err := pgx.Connect(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer cn.Close(ctx)

	var n int
	for range 1_000_000 {
		r, err := cn.Query(context.Background(), "SELECT generate_series(1, 10)")
		if err != nil {
			panic(err)
		}
		for r.Next() {
			err := r.Scan(&n)
			if err != nil {
				panic(err)
			}
		}
		if err := r.Err(); err != nil {
			panic(err)
		}
		r.Close()
	}
}

func doPqlite() {
	ctx := context.Background()
	cn, err := pqlite.Open(ctx, "sslmode=disable", nil)
	if err != nil {
		panic(err)
	}
	defer cn.Close()

	for range 1_000_000 {
		for r, err := range cn.Query(ctx, "SELECT generate_series(1, 10)") {
			if err != nil {
				panic(err)
			}
			_, err := r.ParseInt()
			if err != nil {
				panic(err)
			}
		}
	}
}
