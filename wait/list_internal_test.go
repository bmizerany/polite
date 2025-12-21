package wait

import (
	"context"
	"testing"
	"testing/synctest"
)

// TestTakeNearMiss tests the near-miss scenario where a value arrives
// just as the context is being canceled. This test uses the internal
// testHookWaiterCanceled field to reliably induce the race condition.
func TestTakeNearMiss(t *testing.T) {
	shouldNotCall := func() (int, error) { panic("should not call load func") }
	fourtyTwo := func() (int, error) { return 42, nil }

	synctest.Test(t, func(t *testing.T) {
		p := &List[int]{
			MaxItems:   1,
			MaxWaiters: 10,

			// induce near miss
			testHookWaiterCanceled: func(ch chan int) { ch <- 42 },
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Drain to force waiters to durably block.
		_, err := p.Take(ctx, fourtyTwo)
		if err != nil {
			t.Fatal("draining:", err)
		}

		// 1. waiter starts waiting
		go func() {
			got, err := p.Take(ctx, shouldNotCall)
			if err != nil {
				t.Errorf("near miss recovery: %v", err)
			}
			defer p.Put(got)
			if got != 42 {
				t.Errorf("near miss recovery: got = %d, want 42", got)
			}
		}()
		synctest.Wait()

		// 2. context is cancelled
		cancel()

		// 3. waiter sees cancellation
		// 4. near miss happens
		// 5. waiter recovers and gets 42
		synctest.Wait()
	})
}
