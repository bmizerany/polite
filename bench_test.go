package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func BenchmarkQuery(b *testing.B) {
	ctx := context.Background()

	b.Run("Extended", func(b *testing.B) {
		c, err := Dial(ctx, mainDSN)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for row, err := range c.QueryAll(ctx, "SELECT $1::integer", "99999999") {
				if err != nil {
					b.Fatal(err)
				}
				_, err := row.Atoi()
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
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
		r, err := conn.Query(context.Background(), "SELECT $1::integer", "99999999")
		if err != nil {
			b.Fatal(err)
		}
		for r.Next() {
			var n int
			if err := r.Scan(&n); err != nil {
				b.Fatal(err)
			}
		}
		if err := r.Err(); err != nil {
			b.Fatal(err)
		}
		r.Close()
	}
}
