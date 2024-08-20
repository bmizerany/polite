package pqlite

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func BenchmarkQuery(b *testing.B) {
	ctx := context.Background()

	query := func(n int) string {
		var sb strings.Builder
		sb.WriteString("SELECT")
		for n := range n {
			if n > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(" 12345")
		}
		return sb.String()
	}

	for n := range 5 {
		dsn := testDSN
		q := query(n)
		b.Run("driver=pqlite/n="+strconv.Itoa(n), func(b *testing.B) {
			cn, err := Open(ctx, dsn, nil)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for row, err := range cn.Query(ctx, q) {
					if err != nil {
						b.Fatal(err)
					}
					for range n {
						_, err := row.View()
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}

	for n := range 5 {
		dsn := testDSN
		q := query(n)
		b.Run("driver=pgx/n="+strconv.Itoa(n), func(b *testing.B) {
			conn, err := pgx.Connect(context.Background(), dsn)
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close(context.Background())

			args := make([]any, n)
			for i := range args {
				args[i] = new([]byte)
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
