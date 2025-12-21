package pqlite

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestFairPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &WaitList[int]{
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
					if !errors.Is(err, ErrMaxWaiters) {
						t.Errorf("err = %v, want ErrMaxWaiters", err)
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
			p := &WaitList[int]{
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
			p := &WaitList[int]{
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
			p := &WaitList[int]{
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
	})
}

func BenchmarkFair(b *testing.B) {
	b.Run("uncontended", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			p := &WaitList[int]{MaxItems: 10}
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

		p := &WaitList[int]{
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
