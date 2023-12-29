package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func BenchmarkQuery(b *testing.B) {
	ctx := context.Background()

	query := func(driver string, n int) (string, string) {
		var sb strings.Builder
		sb.WriteString("SELECT")
		for n := range n {
			if n > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(" 12345")
		}
		name := fmt.Sprintf("QueryParams_%s_%d", driver, n)
		return name, sb.String()
	}

	for n := range 5 {
		name, q := query("PQL", n)
		b.Run(name, func(b *testing.B) {
			c, err := Dial(ctx, mainDSN)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for row, err := range c.QueryAll(ctx, q) {
					if err != nil {
						b.Fatal(err)
					}
					for range n {
						_, err := row.Atoi()
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}

	for n := range 5 {
		name, q := query("PGX", n)
		b.Run(name, func(b *testing.B) {
			conn, err := pgx.Connect(context.Background(), mainDSN+" host=/tmp")
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close(context.Background())

			args := make([]any, n)
			for i := range args {
				args[i] = new(int)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r, err := conn.Query(context.Background(), q)
				if err != nil {
					b.Fatal(err)
				}
				for r.Next() {
					if err := r.Scan(args...); err != nil {
						b.Fatal(err)
					}
				}
				if err := r.Err(); err != nil {
					b.Fatal(err)
				}
				r.Close()
			}
		})
	}
}
