//go:build ignore

package main

import (
	"context"
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

		ReceivedAuthenticationResponse: func(ctx context.Context, r febe.AuthenticationResponse) {
			fmt.Printf("ReceivedAuthenticationResponse: %#v\n", r)
		},

		// ReceivedNotice          func(context.Context, *Notice)
		// ReceivedError           func(context.Context, *Error)
		// ReceivedParameterStatus func(context.Context, Parameter)
		// WroteExecute func(ctx context.Context, portal, statement string, args []string)
	})

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
