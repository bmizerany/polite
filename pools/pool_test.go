package pools

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"blake.io/pqlite/internal/testutil"
)

func TestPoolMaxUseWithWaiter(t *testing.T) {
	t.Parallel()

	p := &Pool{
		MaxOpen: 1,
		MaxUses: 1,
		Open:    testOpen,
		Logger:  testutil.Slogger(t),
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var cc [2]*Conn
	var wg sync.WaitGroup
	for i := range len(cc) {
		wg.Go(func() {
			cn, err := p.Get(ctx)
			if err != nil {
				t.Error(err)
			}
			defer p.Put(cn)
			cc[i] = cn
		})
	}
	wg.Wait()
	if cc[0] == cc[1] {
		t.Fatal("expected different connections")
	}
}

func TestPoolClose(t *testing.T) {
	t.Parallel()

	p := &Pool{
		Open:   testOpen,
		Logger: testutil.Slogger(t),
	}
	defer p.Close()

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

	p := &Pool{
		Open:   testOpen,
		Logger: testutil.Slogger(t),
	}
	defer p.Close()

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
		MaxOpen: 1,
		Open:    func(context.Context) (*Conn, error) { return nil, errors.New("test") },
		Logger:  testutil.Slogger(t),
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

	p := &Pool{
		Logger: testutil.Slogger(t),
		Open:   testOpen,
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}

	p.Put(cn)
	if err := cn.Close(); !errors.Is(err, net.ErrClosed) {
		t.Fatal("expected connection to be closed")
	}
}

func TestPoolGetCloseWithWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		waiting := make(chan struct{})

		p := &Pool{
			MaxOpen: 1,
			Open: func(ctx context.Context) (*Conn, error) {
				// Next is never called until by a client until their waiting
				// chan is already in line, so we know now that someone is
				// waiting, so signal that.
				close(waiting)

				// For the waiter to block until the pool is closed, and then
				// only return an error.
				<-ctx.Done()
				return nil, ctx.Err()
			},
			Logger: testutil.Slogger(t),
		}
		defer p.Close()

		var wg sync.WaitGroup
		for range 3 {
			wg.Go(func() {
				_, err := p.Get(t.Context())
				if !errors.Is(err, ErrPoolClosed) {
					t.Errorf("Get() = %v; want %v", err, ErrPoolClosed)
				}
			})
		}

		select {
		case <-waiting:
			// Close the pool while someone is waiting.
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("expected waiting to be closed")
		}

		wg.Wait()
	})
}

func TestPoolGetMaxWaiters(t *testing.T) {
	t.Parallel()

	const maxWaiters = 3

	var shouldOpenConn atomic.Bool

	var waiters sync.WaitGroup
	waiters.Add(maxWaiters)
	next := func(ctx context.Context) (*Conn, error) {
		if shouldOpenConn.Load() {
			return testOpen(ctx)
		}

		// Next is never called until by a client until their waiting
		// chan is already in line, so we know now that someone is
		// waiting, so signal that.
		waiters.Done()

		// For the waiter to block until the pool is closed, and then
		// only return an error.
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	p := &Pool{
		MaxWaiting: maxWaiters,
		Open:       next,
		Logger:     testutil.Slogger(t),
	}
	defer p.Close()

	var unblocked sync.WaitGroup
	for range maxWaiters {
		unblocked.Go(func() {
			_, err := p.Get(ctx)
			if !errors.Is(err, ErrPoolClosed) {
				panic(err)
			}
		})
	}

	waiters.Wait()

	// Make the next Next call open a connection instead of block.
	shouldOpenConn.Store(true)

	_, err := p.Get(ctx)
	if !errors.Is(err, ErrMaxWaiting) {
		t.Fatalf("Get() = %v; want %v", err, ErrMaxWaiting)
	}
}

func BenchmarkPoolGetPut(b *testing.B) {
	b.ReportAllocs()

	p := &Pool{
		Open:        testOpen,
		MaxOpen:     1<<63 - 1,
		MaxUses:     1<<63 - 1,
		MaxWaiting:  1<<63 - 1,
		MaxLifetime: 1<<63 - 1,
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

	p := &Pool{
		Open:        testOpen,
		MaxOpen:     1<<63 - 1,
		MaxUses:     1<<63 - 1,
		MaxWaiting:  1<<63 - 1,
		MaxLifetime: 1<<63 - 1,
	}
	defer p.Close()

	// Get a conn for setup side effects.
	_, err := p.Get(context.Background())
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		p.startNewConn(nil)
	}
}

func BenchmarkPool(b *testing.B) {
	b.Run("open=1", func(b *testing.B) { benchmarkPool(b, 1) })
	b.Run("open=2", func(b *testing.B) { benchmarkPool(b, 2) })
	b.Run("open=4", func(b *testing.B) { benchmarkPool(b, 4) })
	b.Run("open=8", func(b *testing.B) { benchmarkPool(b, 8) })
	b.Run("open=16", func(b *testing.B) { benchmarkPool(b, 16) })
	b.Run("open=32", func(b *testing.B) { benchmarkPool(b, 32) })
	b.Run("open=48", func(b *testing.B) { benchmarkPool(b, 48) })
	b.Run("open=64", func(b *testing.B) { benchmarkPool(b, 64) })
}

func testOpen(ctx context.Context) (*Conn, error) {
	return &Conn{}, nil
}

func TestPoolSyncExpired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &Pool{
			MaxLifetime: 3 * time.Second,
			Logger:      testutil.Slogger(t),
			Open:        testOpen,
		}
		defer p.Close()

		ctx := context.Background()

		cn0, err := p.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}

		sleepPutGet := func() *Conn {
			t.Helper()
			time.Sleep(1 * time.Second)
			p.Put(cn0)
			got, err := p.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return got
		}

		for range 2 {
			if sleepPutGet() != cn0 {
				t.Fatal("expected starting *Conn")
			}
		}

		if sleepPutGet() == cn0 {
			t.Fatal("expected new conn")
		}

		// Recycle current Conn
		cc := sleepPutGet()
		p.Put(cc)

		time.Sleep(3 * time.Second)
		p.CloseExpiredConns()
		if sleepPutGet() == cc {
			t.Fatal("expected new conn")
		}
	})
}

func TestPoolSyncGetOpenError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var retries atomic.Int64

		p := &Pool{
			Logger: testutil.Slogger(t),
			Open: func(context.Context) (*Conn, error) {
				retries.Add(1)
				return nil, errors.New("test")
			},
		}
		defer p.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var got [3]error
		var wg sync.WaitGroup
		for i := range 3 {
			wg.Go(func() {
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

		time.Sleep(3 * time.Second)
		wg.Wait()

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

func (p *Pool) checkAndDrainPool() {
	for {
		v := p.connChanPool.Get()
		if v == nil {
			break
		}
		ch := v.(chan *Conn) // panic if wrong type
		if len(ch) > 0 {
			panic(fmt.Errorf("non-empty Conn in pool: len = %d!", len(ch)))
		}
	}
}

func TestPoolSyncGetNearMissContextCancel(t *testing.T) {
	check := checker(t)

	synctest.Test(t, func(t *testing.T) {
		p := &Pool{
			Logger:  testutil.Slogger(t),
			Open:    testOpen,
			MaxOpen: 1,
		}
		defer p.Close()
		defer p.checkAndDrainPool()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Acquire the only allowed Conn in the pool
		want, err := p.Get(ctx)
		check(err)

		p.testHookWaiterCanceled = func(ch chan *Conn) {
			// Induce a near miss. Send the Conn on the wait
			// channel after the context is canceled, but before
			// Get checks the wait chan for a near miss after
			// removing itself from line, but before returning.
			ch <- want
		}

		ch := make(chan *Conn, 1)
		go func() {
			cn, err := p.Get(ctx)
			check(err)
			ch <- cn
		}()

		// Run until Get reaches the select; without this we risk
		// getting a false positive if the initial context error check
		// in Get is scheduled after we cancel the context.
		synctest.Wait()

		// cancel the context causing it to fall into the <-ctx.Done() case
		cancel()

		got := <-ch
		if got != want {
			t.Fatal("expected near miss recovery")
		}

		p.Put(got)

		// A canceled context should not be able to acquire the Conn
		_, err = p.Get(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v; want %v", err, context.Canceled)
		}
	})
}

func TestPoolSyncGetNearMissPoolClosed(t *testing.T) {
	check := checker(t)

	synctest.Test(t, func(t *testing.T) {
		p := &Pool{
			Logger:  testutil.Slogger(t),
			Open:    testOpen,
			MaxOpen: 1,
		}
		defer p.Close()
		defer p.checkAndDrainPool()

		// Acquire the only allowed Conn in the pool
		_, err := p.Get(t.Context())
		check(err)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		p.testHookWaiterCanceled = func(ch chan *Conn) {
			p.Close()
		}

		ch := make(chan error, 1)
		go func() {
			cn, err := p.Get(ctx)
			if cn != nil {
				t.Error("expected nil *Conn")
			}
			ch <- err
		}()

		// Run until Get reaches the select,
		// cancel the context,
		// and then wait for the Get goroutine to exit.
		synctest.Wait()

		cancel()

		getErr := <-ch
		if errors.Is(getErr, ErrPoolClosed) {
			t.Fatal("Get is ErrPoolClosed (but should not be)")
		}
		if !errors.Is(getErr, context.Canceled) {
			t.Fatalf("err = %v; want %v", getErr, context.Canceled)
		}
	})
}

func checker(t *testing.T) func(err error) {
	t.Helper()
	return func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func benchmarkPool(b *testing.B, maxOpen int) {
	p := &Pool{
		MaxOpen: maxOpen,
		Open:    testOpen,
		Logger:  testutil.Slogger(b),
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
