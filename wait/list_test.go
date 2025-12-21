package wait_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"blake.io/pqlite/wait"
)

func TestFairPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &wait.List[int]{
			MaxItems:   2,
			MaxWaiters: 3,
		}

		loads := new(atomic.Int64)
		checkTake := func(want int) {
			t.Helper()
			got, err := p.Take(t.Context(), func() int {
				return int(loads.Add(1) - 1)
			})
			if err != nil {
				if want < 0 {
					if !errors.Is(err, wait.ErrMaxWaiters) {
						t.Errorf("err = %v, want wait.ErrMaxWaiters", err)
					}
					return
				}
				t.Error("unexpected error taking from pool:", err)
			}
			if got != want {
				t.Errorf("got = %d, want %d", got, want)
			}
		}

		checkLoads := func(want int64) {
			t.Helper()
			if got := loads.Load(); got != want {
				t.Errorf("loads = %d, want %d", got, want)
			}
		}

		checkTake(0)
		p.Put(0)

		checkTake(0)
		checkTake(1)

		checkLoads(2)

		for i := range 3 {
			go checkTake(i % 2)
			synctest.Wait()
		}

		checkTake(-1) // unblocked still increments

		checkLoads(2)

		p.Put(0)
		synctest.Wait()
		checkLoads(2)

		p.Put(1)
		synctest.Wait()
		checkLoads(2)

		p.Put(0)
		synctest.Wait()
		checkLoads(2)
	})
}

func TestFairPoolTakeContextCancel(t *testing.T) {
	// load funcs for testing
	shouldNotCall := func() int { panic("should not call load func") }
	fourtyTwo := func() int { return 42 }

	t.Run("early cancel", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := &wait.List[int]{
				MaxItems:   1,
				MaxWaiters: 10,
			}

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			_, err := p.Take(ctx, shouldNotCall)
			if !errors.Is(err, context.Canceled) {
				t.Errorf("err = %v, want context.Canceled", err)
			}
		})
	})

	t.Run("waiting cancel", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := &wait.List[int]{
				MaxItems:   1,
				MaxWaiters: 10,
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
				if !errors.Is(err, context.Canceled) {
					t.Errorf("waiting cancel: err = %v, want context.Canceled (got = %v)", got, err)
				}
			}()

			// 2. waiter is durably blocked
			synctest.Wait()

			// 3. context is cancelled
			cancel()

			// 4. waiter sees cancellation
			synctest.Wait()
		})
	})

	t.Run("near miss", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := &wait.List[int]{
				MaxItems:   1,
				MaxWaiters: 10,

				// induce near miss
				TestHookWaiterCanceled: func(ch chan int) { ch <- 42 },
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
	})
}

func BenchmarkFair(b *testing.B) {
	b.Run("uncontended", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			p := &wait.List[int]{MaxItems: 10}
			for pb.Next() {
				v, err := p.Take(context.Background(), func() int { return 42 })
				if err != nil {
					b.Fatal("Take:", err)
				}
				if v != 42 {
					b.Fatalf("got %d, want 42", v)
				}
				p.Put(v)
			}
		})
	})

	b.Run("contended", func(b *testing.B) {
		b.ReportAllocs()

		var tttt atomic.Int64 // total-time-to-take
		var tttp atomic.Int64 // total-time-to-put

		p := &wait.List[int]{
			MaxItems:   10,
			MaxWaiters: 100,
		}

		b.RunParallel(func(pb *testing.PB) {
			zzz := rand.NewZipf(rand.New(rand.NewPCG(1, 2)), 2.0, 1.5, 100)
			for pb.Next() {
				ttt := time.Now()
				h, err := p.Take(context.Background(), func() int { return 42 })
				if err != nil {
					b.Fatal("Take:", err)
				}
				tttt.Add(time.Since(ttt).Nanoseconds())

				if h != 42 {
					b.Fatalf("got = %d, want 42", h)
				}

				// "work"
				time.Sleep(time.Microsecond * time.Duration(zzz.Uint64()))

				ttp := time.Now()
				p.Put(h)
				tttp.Add(time.Since(ttp).Nanoseconds())
			}
		})

		b.ReportMetric(float64(tttt.Load())/float64(b.N), "ns/take")
		b.ReportMetric(float64(tttp.Load())/float64(b.N), "ns/put")
	})
}

func TestWaitListClose(t *testing.T) {
	t.Run("Put after Close does nothing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := &wait.List[int]{
				MaxItems:   10,
				MaxWaiters: 10,
			}

			// Close the waitlist
			p.Close()

			// Put should do nothing when closed
			p.Put(42)

			// Verify nothing was added
			_, ok := p.TryTake()
			if ok {
				t.Error("TryTake() should return ok=false after Close, but got a value")
			}
		})
	})

	t.Run("concurrent Close and Put", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := &wait.List[int]{
				MaxItems:   10,
				MaxWaiters: 10,
			}

			// Start multiple Puts and a Close concurrently
			for i := range 5 {
				i := i
				go func() {
					p.Put(i)
				}()
			}

			go func() {
				p.Close()
			}()

			// Let all goroutines run
			synctest.Wait()

			// After Close, new Puts should do nothing
			p.Put(999)

			// Drain whatever was put before close
			count := 0
			for {
				_, ok := p.TryTake()
				if !ok {
					break
				}
				count++
			}

			// We should have gotten some items (0-5), but not 999
			if count > 5 {
				t.Errorf("got %d items, but put at most 5 before close", count)
			}
		})
	})

	t.Run("Close wakes up waiters", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := &wait.List[int]{
				MaxItems:   1,
				MaxWaiters: 10,
			}

			ctx := context.Background()

			// Start a waiter
			errCh := make(chan error, 1)
			go func() {
				_, err := p.Wait(ctx)
				errCh <- err
			}()

			// Wait for the goroutine to block
			synctest.Wait()

			// Close should wake it up
			p.Close()

			// Waiter should get wait.ErrClosed
			synctest.Wait()

			err := <-errCh
			if !errors.Is(err, wait.ErrClosed) {
				t.Errorf("Wait() err = %v, want wait.ErrClosed", err)
			}
		})
	})

	t.Run("Put during Close with waiter", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := &wait.List[int]{
				MaxItems:   1,
				MaxWaiters: 10,
			}

			ctx := context.Background()

			// Start a waiter
			valCh := make(chan int, 1)
			errCh := make(chan error, 1)
			go func() {
				v, err := p.Wait(ctx)
				if err != nil {
					errCh <- err
				} else {
					valCh <- v
				}
			}()

			// Wait for the goroutine to block
			synctest.Wait()

			// Race: Put and Close happen concurrently
			go p.Put(42)
			go p.Close()

			// Let both run
			synctest.Wait()

			// Waiter should get either the value or wait.ErrClosed
			select {
			case v := <-valCh:
				// Got the value - this is fine if Put won the race
				if v != 42 {
					t.Errorf("got value %d, want 42", v)
				}
			case err := <-errCh:
				// Got an error - this is fine if Close won the race
				if !errors.Is(err, wait.ErrClosed) {
					t.Errorf("Wait() err = %v, want wait.ErrClosed", err)
				}
			default:
				t.Error("waiter should have been unblocked")
			}
		})
	})

	t.Run("Wait after Close returns wait.ErrClosed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := &wait.List[int]{}

			p.Close()

			_, err := p.Wait(context.Background())
			if !errors.Is(err, wait.ErrClosed) {
				t.Errorf("Wait() err = %v, want wait.ErrClosed", err)
			}
		})
	})

	t.Run("Close is idempotent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := &wait.List[int]{}

			// Close multiple times - should not panic
			p.Close()
			p.Close()
			p.Close()

			// Should still return wait.ErrClosed
			_, err := p.Wait(context.Background())
			if !errors.Is(err, wait.ErrClosed) {
				t.Errorf("Wait() err = %v, want wait.ErrClosed", err)
			}
		})
	})
}
