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

	for n := range 10 {
		var sb strings.Builder
		sb.WriteString("SELECT")
		for n := range n {
			if n > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(" 12345")
		}
		q := sb.String()

		c, err := Dial(ctx, mainDSN)
		if err != nil {
			b.Fatal(err)
		}

		name := fmt.Sprintf("QueryParams_PGL_%d", n)
		b.Run(name, func(b *testing.B) {
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
}

func BenchmarkPGX(b *testing.B) {
	conn, err := pgx.Connect(context.Background(), mainDSN+" host=/tmp")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(context.Background())

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r, err := conn.Query(context.Background(), "SELECT $1::integer, 2", "12345")
		if err != nil {
			b.Fatal(err)
		}
		for r.Next() {
			var n int
			if err := r.Scan(&n, &n); err != nil {
				b.Fatal(err)
			}
		}
		if err := r.Err(); err != nil {
			b.Fatal(err)
		}
		r.Close()
	}
}
