package polite

import (
	"context"
	json "encoding/json/v2"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkWideRow compares the two ways to fan a row into a Go struct
// over 1,000 wide rows: per-column decoders versus a single row_to_json
// payload unmarshaled with encoding/json/v2. The projection covers one
// column per decoder family (int8, float8, text, timestamptz, bytea).
//
// Run as:
//
//	go test -bench=BenchmarkWideRow -benchmem -count=10 ./... > old.txt
//	# (change something)
//	go test -bench=BenchmarkWideRow -benchmem -count=10 ./... > new.txt
//	benchstat old.txt new.txt
func BenchmarkWideRow(b *testing.B) {
	ctx := b.Context()
	cn, err := Dial(ctx, testDSN, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer cn.Close()

	type Row struct {
		ID      int64     `json:"id"`
		Name    string    `json:"name"`
		Amount  float64   `json:"amount"`
		TS      time.Time `json:"ts"`
		Payload []byte    `json:"payload"` // json/v2 decodes []byte from base64
	}

	const q = `
		SELECT
			i::int8                                                  AS id,
			('item-' || i::text)                                     AS name,
			(i::float8 * 1.5)                                        AS amount,
			('2026-01-01 00:00:00+00'::timestamptz
				+ i * '1 second'::interval)                          AS ts,
			encode(decode(lpad(to_hex(i), 8, '0'), 'hex'), 'base64') AS payload
		FROM generate_series(1, 1000) AS i
	`

	b.Run("Columns", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for row := range cn.Query(ctx, q) {
				var r Row
				r.ID, _ = row.Column(0).DecodeInt()
				r.Name, _ = row.Column(1).DecodeText()
				r.Amount, _ = row.Column(2).DecodeFloat()
				r.TS, _ = row.Column(3).DecodeTime()
				r.Payload, _ = row.Column(4).Bytes()
			}
		}
	})

	b.Run("ColumnsByName", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for row := range cn.Query(ctx, q) {
				var r Row
				for col := range row.Columns() {
					switch col.Name() {
					case "id":
						r.ID, _ = col.DecodeInt()
					case "name":
						r.Name, _ = col.DecodeText()
					case "amount":
						r.Amount, _ = col.DecodeFloat()
					case "ts":
						r.TS, _ = col.DecodeTime()
					case "payload":
						r.Payload, _ = col.Bytes()
					}
				}
			}
		}
	})

	b.Run("JSON", func(b *testing.B) {
		const jsonQuery = `SELECT row_to_json(t) FROM (` + q + `) AS t`
		b.ReportAllocs()
		for b.Loop() {
			for row := range cn.Query(ctx, jsonQuery) {
				payload, _ := row.Column(0).Bytes()
				var r Row
				if err := json.Unmarshal(payload, &r); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func BenchmarkQueryDecodeInt(b *testing.B) {
	ctx := b.Context()
	cn, err := Dial(ctx, testDSN, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer cn.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		for range 10 {
			for row := range cn.Query(ctx, "SELECT generate_series(1, 10)") {
				if _, err := row.Column(0).DecodeInt(); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

// BenchmarkPoolContention tests the pool under high contention
// to measure variance in latency, which is a key metric for fairness.
func BenchmarkPoolContention(b *testing.B) {
	p := &Pool{
		Dial:       testDialer,
		MaxItems:   10,
		MaxWaiters: 100,
	}
	defer p.Close()

	// Pre-warm the pool
	for range 10 {
		cn, err := p.Get(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		p.Put(cn)
	}

	b.ReportAllocs()
	b.ResetTimer()

	var getTimes atomic.Int64
	var putTimes atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		zzz := rand.NewZipf(rand.New(rand.NewPCG(1, 2)), 2.0, 1.5, 100)
		for pb.Next() {
			start := time.Now()
			cn, err := p.Get(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			getTimes.Add(time.Since(start).Nanoseconds())

			// Simulate work with variable duration
			time.Sleep(time.Microsecond * time.Duration(zzz.Uint64()))

			start = time.Now()
			p.Put(cn)
			putTimes.Add(time.Since(start).Nanoseconds())
		}
	})

	b.ReportMetric(float64(getTimes.Load())/float64(b.N), "ns/get")
	b.ReportMetric(float64(putTimes.Load())/float64(b.N), "ns/put")
}
