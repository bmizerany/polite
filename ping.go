//go:build ignore

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"time"

	"blake.io/postgres"
	"blake.io/postgres/febe"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctx = febe.WithTrace(ctx, &febe.Trace{
		ReceivedHeader: func(ctx context.Context, h febe.MessageHeader) {
			fmt.Printf("ReceivedHeader: %s\n", h)
		},
	})

	f, err := os.OpenFile("trace.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// TLS keylog recroding
	cfg := &postgres.Config{
		TLSConfig: &tls.Config{
			KeyLogWriter: f,
		},
	}

	cn, err := postgres.Dial(ctx, os.Getenv("DATABASE_URL"), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer cn.Close()

	fmt.Println("PING")
	defer fmt.Println("PONG")
	if err := postgres.Ping(ctx, cn); err != nil {
		log.Fatal(err)
	}
}
