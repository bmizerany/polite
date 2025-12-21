package wait_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"blake.io/pqlite/wait"
)

// TestCancellationContentionStress measures wall-clock time for concurrent
// cancellations to verify O(n) DeleteFunc doesn't cause quadratic blowup.
func TestCancellationContentionStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	sizes := []int{100, 500, 1000, 2000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d_waiters", size), func(t *testing.T) {
			p := &wait.List[int]{
				MaxItems:   1,
				MaxWaiters: size * 2,
			}

			// Block the queue so all waiters will timeout
			p.Put(42)
			first, _ := p.TryTake()
			_ = first

			var wg sync.WaitGroup
			start := time.Now()

			// All waiters will timeout and cancel concurrently
			for i := 0; i < size; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Microsecond)
					defer cancel()
					_, _ = p.Take(ctx, func() (int, error) {
						return 0, fmt.Errorf("should not load")
					})
				}()
			}

			wg.Wait()
			elapsed := time.Since(start)

			nsPerCancel := elapsed.Nanoseconds() / int64(size)
			t.Logf("%d waiters: %v total, %v ns/cancel", size, elapsed, nsPerCancel)

			// If we had O(n²) behavior with concurrent cancellations all hitting
			// the O(n) DeleteFunc, we'd see ns/cancel grow linearly with size.
			// For example, if 100 waiters takes 1ms total, 1000 waiters would
			// take ~100ms if quadratic (not just 10ms).
			//
			// Set a generous timeout to catch catastrophic quadratic behavior
			// while allowing for normal variance.
			maxAllowed := 10 * time.Millisecond * time.Duration(size) / 100
			if elapsed > maxAllowed {
				t.Errorf("took %v for %d cancellations, expected < %v (possible O(n²) behavior)",
					elapsed, size, maxAllowed)
			}
		})
	}
}
