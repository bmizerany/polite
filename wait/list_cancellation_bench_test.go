package wait_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"blake.io/pqlite/wait"
)

// BenchmarkHighCancellationContention tests the worst-case scenario:
// - MaxWaiters reached (1000 waiters)
// - 90% of waiters cancel their context
// - Very few items actually available (1:100 ratio)
// - All cancellations happen concurrently
//
// This stresses the O(n) DeleteFunc path in handleCancel.
func BenchmarkHighCancellationContention(b *testing.B) {
	const (
		maxWaiters      = 1000
		cancellationPct = 90 // 90% of waiters will cancel
	)

	p := &wait.List[int]{
		MaxItems:   1,
		MaxWaiters: maxWaiters,
	}

	// Consume the one allowed item slot by having one successful Take
	loadCalled := atomic.Bool{}
	loadOnce := func() (int, error) {
		if loadCalled.CompareAndSwap(false, true) {
			time.Sleep(1 * time.Millisecond) // Give time for other waiters to queue up
			return 42, nil
		}
		// Should never get here
		b.Fatal("load called more than once")
		return 0, nil
	}

	// Start one Take that will consume the MaxItems slot
	var firstWg sync.WaitGroup
	firstWg.Add(1)
	go func() {
		defer firstWg.Done()
		v, err := p.Take(context.Background(), loadOnce)
		if err != nil {
			b.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond) // Hold onto it for a while
		p.Put(v)
	}()

	// Wait for the load to start
	time.Sleep(2 * time.Millisecond)

	// Now load should never be called for subsequent waiters
	load := func() (int, error) {
		b.Fatal("load should not be called for subsequent waiters")
		return 0, nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		var cancelCount atomic.Int64

		// Spawn maxWaiters goroutines that will all timeout
		for j := 0; j < maxWaiters; j++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				// All contexts will timeout
				ctx, cancelFunc := context.WithTimeout(context.Background(), 100*time.Microsecond)
				defer cancelFunc()

				_, err := p.Take(ctx, load)
				if err != nil {
					if err == context.DeadlineExceeded || err == context.Canceled {
						cancelCount.Add(1)
					}
				}
			}()
		}

		wg.Wait()

		// Verify we got the cancellations
		if cancelCount.Load() < int64(maxWaiters*95/100) {
			b.Fatalf("expected ~%d cancellations, got %d", maxWaiters, cancelCount.Load())
		}
	}

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(maxWaiters), "ns/waiter")
}

// BenchmarkCancellationScaling tests how cancellation performance scales
// with the number of waiters.
func BenchmarkCancellationScaling(b *testing.B) {
	sizes := []int{10, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(string(rune(size)), func(b *testing.B) {
			p := &wait.List[int]{
				MaxItems:   1,
				MaxWaiters: size * 2,
			}

			load := func() (int, error) {
				return 42, nil
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup

				// All waiters will cancel
				for j := 0; j < size; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
						defer cancel()
						_, _ = p.Take(ctx, load)
					}()
				}

				wg.Wait()
			}

			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(size), "ns/cancel")
		})
	}
}
