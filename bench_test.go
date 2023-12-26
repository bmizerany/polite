package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func BenchmarkQuery(b *testing.B) {
	ctx := context.Background()

	b.Run("Simple", func(b *testing.B) {
		c := newConn(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := c.Query(ctx, "SELECT 1").Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Extended", func(b *testing.B) {
		c := newConn(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := c.Query(ctx, "SELECT $1", "1").Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPGX(b *testing.B) {
	dsn := createDB(b)

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(context.Background())

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err := conn.QueryRow(context.Background(), "SELECT $1", "1").Scan(nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
