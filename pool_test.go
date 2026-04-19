package polite

import (
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"blake.io/polite/dsn"
	"blake.io/polite/internal/testutil"
)

func newTestPool(t *testing.T) *Pool {
	p := &Pool{
		Logger: testutil.Slogger(t),
		Dial:   testDialer,
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestDefaultDSNUsesVerifyFull(t *testing.T) {
	o, err := dsn.ParseOptions(DefaultDSN)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := o.SSLMode, "verify-full"; got != want {
		t.Fatalf("sslmode = %q; want %q", got, want)
	}
}

func TestPoolClose(t *testing.T) {
	t.Parallel()

	p := newTestPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := p.Close(); err != nil {
		t.Fatal("error on first close:", err)
	}
	cn, err := p.Get(ctx)
	if err == nil {
		cn.Close()
		t.Fatal("Get() err = nil; want error")
	}
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Get() err = %v; want %v", err, ErrPoolClosed)
	}
}

func TestPoolGetReady(t *testing.T) {
	t.Parallel()

	p := newTestPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.Put(cn)

	cn2, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cn != cn2 {
		t.Fatalf("expected same connection, got %p and %p", cn, cn2)
	}
}

func TestPoolGetNextErrorAfterClose(t *testing.T) {
	t.Parallel()

	p := &Pool{
		MaxItems: 1,
		Dial:     func(context.Context) (*Conn, error) { return nil, errors.New("test") },
		Logger:   testutil.Slogger(t),
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	cn, err := p.Get(ctx)
	if err == nil {
		cn.Close()
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
}

func TestPoolPutClosedPool(t *testing.T) {
	t.Parallel()

	p := newTestPool(t)

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}

	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("expected connection alive after pool close, got %v", err)
	}
	p.Put(cn)
	cn.Close()
}

func TestPoolGetMaxWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newTestPool(t)
		p.MaxItems = 1
		p.MaxWaiters = 1
		defer func() {
			p.Close()       // Unblock Waiter 1
			synctest.Wait() // Allow Waiter 1 to finish after unblocking
		}()

		// take the only conn
		_, err := p.Get(t.Context())
		if err != nil {
			t.Fatalf("Occupy Get() = %v; want nil", err)
		}

		// occupy the only waiting slot and wait until durably blocked.
		go func() {
			_, err := p.Get(t.Context())
			if !errors.Is(err, ErrPoolClosed) {
				t.Errorf("Waiter 1 Get() = %v; want %v", err, ErrPoolClosed)
			}
		}()
		synctest.Wait()

		// check any additional waiters are rejected with ErrPoolFull
		for range 3 {
			_, err = p.Get(t.Context())
			if !errors.Is(err, ErrPoolFull) {
				t.Errorf("Waiter 2 Get() = %v; want %v", err, ErrPoolFull)
			}
		}
	})
}

func TestPoolGetPrecanceledContext(t *testing.T) {
	t.Parallel()

	var dials atomic.Int64
	p := &Pool{
		Logger: testutil.Slogger(t),
		Dial: func(context.Context) (*Conn, error) {
			dials.Add(1)
			panic("dial should not be called for a pre-canceled context")
		},
	}
	t.Cleanup(func() { p.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	cn, err := p.Get(ctx)
	if cn != nil {
		cn.Close()
		t.Fatalf("Get() cn = %p; want nil", cn)
	}
	if err == nil {
		t.Fatal("Get() err = nil; want non-nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Get() err = %v; want wrap of %v", err, context.Canceled)
	}
	if err != ctx.Err() {
		t.Fatalf("Get() err = %v; want ctx.Err() = %v", err, ctx.Err())
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("dials = %d; want 0 (short-circuit should skip dialing)", n)
	}
}

func TestPoolGetCanceledWhileWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scene: a pool with room for exactly one connection, already
		// occupied. A second Get must block on the waitlist until the
		// first conn is returned.
		p := newTestPool(t)
		p.MaxItems = 1

		cn, err := p.Get(t.Context())
		if err != nil {
			t.Fatalf("initial Get() = %v; want nil", err)
		}

		// Second Get blocks in the waitlist. Cancel its context and
		// observe the wrapped ctx error bubbling out of Take.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var (
			waiterCn  *Conn
			waiterErr error
		)
		done := make(chan struct{})
		go func() {
			defer close(done)
			waiterCn, waiterErr = p.Get(ctx)
		}()

		// Let the second Get reach a durable block inside Take.
		synctest.Wait()

		cancel()

		// Wait for the goroutine to observe the cancellation.
		synctest.Wait()
		<-done

		if waiterCn != nil {
			t.Fatalf("waiter Get() cn = %p; want nil", waiterCn)
		}
		if !errors.Is(waiterErr, context.Canceled) {
			t.Fatalf("waiter Get() err = %v; want wrap of %v", waiterErr, context.Canceled)
		}

		// The outstanding conn is still usable: returning it and
		// re-taking it should yield the same conn.
		p.Put(cn)
		cn2, err := p.Get(t.Context())
		if err != nil {
			t.Fatalf("re-Get() = %v; want nil", err)
		}
		if cn2 != cn {
			t.Fatalf("re-Get() cn = %p; want %p", cn2, cn)
		}
	})
}

func TestPoolPutAfterCanceledQuery(t *testing.T) {
	t.Parallel()

	p := newTestPool(t)
	p.MaxItems = 1

	cn, err := p.Get(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Run a long query under a short deadline and range to surface the
	// cancellation through Row.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	var gotErr error
	for row := range cn.Query(ctx, "SELECT pg_sleep(1)") {
		if _, err := row.Column(0).Bytes(); err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatal("expected deadline error from canceled query")
	}
	if !errors.Is(gotErr, context.DeadlineExceeded) {
		t.Fatalf("Query() err = %v; want wrap of %v", gotErr, context.DeadlineExceeded)
	}

	// Put must accept the conn as idle and hand it back on the next Get.
	p.Put(cn)

	cn2, err := p.Get(t.Context())
	if err != nil {
		t.Fatalf("Get() after Put() = %v; want nil", err)
	}
	if cn2 != cn {
		t.Fatalf("Get() cn = %p; want same conn %p after cancellation recovery", cn2, cn)
	}
	if err := cn2.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) on reused conn = %v; want nil", err)
	}
	p.Put(cn2)
}

func TestPoolGetAllocations(t *testing.T) {
	t.Skip("TODO: unskip and fix after synctest problems are fixed")

	p := &Pool{
		Dial: testDialer,
	}
	defer p.Close()

	got := testing.AllocsPerRun(1000, func() {
		cn, err := p.Get(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		p.Put(cn)
	})
	if got > 0 {
		t.Fatalf("unexpected allocations: %v", got)
	}
}

func BenchmarkPoolGetPut(b *testing.B) {
	b.ReportAllocs()

	p := &Pool{
		Dial: testDialer,

		MaxItems:   1<<63 - 1,
		MaxWaiters: 1<<63 - 1,
	}
	defer p.Close()

	for b.Loop() {
		ctx := context.Background()
		_, err := p.Get(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPoolStartNewConn(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		p := &Pool{
			Dial:       testDialer,
			MaxItems:   1,
			MaxWaiters: 1,
		}
		cn, err := p.Get(context.Background())
		if err != nil {
			p.Close()
			b.Fatal(err)
		}
		cn.Close()
		if err := p.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPoolSyncGetOpenError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var retries atomic.Int64

		p := &Pool{
			Logger: testutil.Slogger(t),
			Dial: func(context.Context) (*Conn, error) {
				retries.Add(1)
				return nil, errors.New("test")
			},
		}
		defer p.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var got [3]error
		var g sync.WaitGroup
		for i := range 3 {
			g.Go(func() {
				cn, err := p.Get(ctx)
				got[i] = err
				if err == nil {
					panic("expected error")
				}
				if cn != nil {
					cn.Close()
					panic("expected nil conn")
				}
			})
		}

		sleepUntilDeadline(ctx)

		g.Wait()

		for i := range len(got) {
			if got[i] == nil {
				t.Fatalf("[%d]: unexpected nil error", i)
			}
		}

		if retries.Load() <= int64(len(got)) {
			t.Fatalf("retries = %d; want > 3", retries.Load())
		}
	})
}

func sleepUntilDeadline(ctx context.Context) {
	d, ok := ctx.Deadline()
	if !ok {
		panic("no deadline")
	}
	time.Sleep(time.Until(d))
}

func BenchmarkPool(b *testing.B) {
	b.Run("driver=polite/open=1", func(b *testing.B) { benchmarkPool(b, 1) })
	b.Run("driver=polite/open=2", func(b *testing.B) { benchmarkPool(b, 2) })
	b.Run("driver=polite/open=4", func(b *testing.B) { benchmarkPool(b, 4) })
	b.Run("driver=polite/open=8", func(b *testing.B) { benchmarkPool(b, 8) })
	b.Run("driver=polite/open=16", func(b *testing.B) { benchmarkPool(b, 16) })
	b.Run("driver=polite/open=32", func(b *testing.B) { benchmarkPool(b, 32) })
	b.Run("driver=polite/open=48", func(b *testing.B) { benchmarkPool(b, 48) })
	b.Run("driver=polite/open=64", func(b *testing.B) { benchmarkPool(b, 64) })
}

func benchmarkPool(b *testing.B, maxItems int) {
	p := &Pool{
		MaxItems: maxItems,
		Dial:     testDialer,
		Logger:   testutil.Slogger(b),
	}
	defer p.Close()

	ctx := context.Background()

	var getns atomic.Int64 // time to get
	maxgets := make([]time.Duration, runtime.NumCPU())
	var putns atomic.Int64 // time to put
	maxputs := make([]time.Duration, runtime.NumCPU())

	var rem atomic.Int64
	rem.Store(int64(b.N))
	var wg sync.WaitGroup
	for i := range runtime.NumCPU() {
		wg.Go(func() {
			zzz := rand.NewZipf(rand.New(rand.NewPCG(1, 2)), 2.0, 1.5, 100)
			for rem.Add(-1) >= 0 {
				start := time.Now()
				cn, err := p.Get(ctx)
				if err != nil {
					panic(err)
				}
				d := time.Since(start)
				getns.Add(d.Nanoseconds())
				maxgets[i] = max(maxgets[i], d)

				time.Sleep(time.Duration(zzz.Uint64()) * time.Millisecond)

				start = time.Now()
				p.Put(cn)
				d = time.Since(start)
				putns.Add(d.Nanoseconds())
				maxputs[i] = max(maxputs[i], d)
			}
		})
	}
	wg.Wait()

	b.ReportMetric(float64(getns.Load())/float64(b.N), "ns/get")
	b.ReportMetric(float64(slices.Max(maxgets)), "ns/getmax")

	b.ReportMetric(float64(putns.Load())/float64(b.N), "ns/put")
	b.ReportMetric(float64(slices.Max(maxputs)), "ns/putmax")
}

func testDialer(ctx context.Context) (*Conn, error) {
	return Dial(ctx, testDSN, nil)
}
