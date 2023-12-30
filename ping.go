//go:build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"blake.io/postgres"
)

func main() {
	ctx := context.Background()
	cn, err := postgres.Dial(ctx, os.Getenv("DATABASE_URL"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	fmt.Println("PING")
	defer fmt.Println("PONG")
	if err := cn.Ping(ctx); err != nil {
		log.Fatal(err)
	}
}
