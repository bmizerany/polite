package polite

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkPoolContention tests the pool under high contention
// to measure variance in latency, which is a key metric for fairness.
func BenchmarkPoolContention(b *testing.B) {
	p := &Pool{
		Open: func(context.Context) (*Conn, error) {
			return &Conn{closed: true}, nil
		},
		MaxOpen:     10,
		MaxWaiting:  100,
		MaxUses:     1<<63 - 1,
		MaxLifetime: 1<<63 - 1,
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
