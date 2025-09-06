package pqlite

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"blake.io/pqlite/internal/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPoolMaxUseWithWaiter(t *testing.T) {
	t.Parallel()

	p := &Pool{
		MaxOpen: 1,
		MaxUses: 1,
		Open:    Opener(testDSN, nil),
		Logger:  testutil.Slogger(t),
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var cc [2]*Conn
	var wg sync.WaitGroup
	for i := range len(cc) {
		wg.Go(func() {
			defer wg.Done()
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
		Open:   Opener(testDSN, nil),
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
		Open:   Opener(testDSN, nil),
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
		Open:   Opener(testDSN, nil),
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

	if err := cn.Ping(ctx); err != nil {
		t.Fatalf("expected connection alive after pool close, got %v", err)
	}
	p.Put(cn)
	if err := cn.Close(); !errors.Is(err, net.ErrClosed) {
		t.Fatal("expected connection to be closed")
	}
}

func TestPoolGetCloseWithWaiter(t *testing.T) {
	waiting := make(chan struct{})
	next := func(ctx context.Context) (*Conn, error) {
		// Next is never called until by a client until their waiting
		// chan is already in line, so we know now that someone is
		// waiting, so signal that.
		close(waiting)

		// For the waiter to block until the pool is closed, and then
		// only return an error.
		<-ctx.Done()
		return nil, ctx.Err()
	}

	p := &Pool{
		MaxOpen: 1,
		Open:    next,
		Logger:  testutil.Slogger(t),
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var got [3]error
	var wg sync.WaitGroup
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cn, err := p.Get(ctx)
			if err != nil {
				got[i] = err
				return
			}
			p.Put(cn)
		}()
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

	want := [...]error{ErrPoolClosed, ErrPoolClosed, ErrPoolClosed}
	if got != want {
		t.Fatalf("got = %v; want %v", got, want)
	}
}

func TestPoolGetMaxWaiters(t *testing.T) {
	t.Parallel()

	const maxWaiters = 3

	var shouldOpenConn atomic.Bool

	var waiters sync.WaitGroup
	waiters.Add(maxWaiters)
	next := func(ctx context.Context) (*Conn, error) {
		if shouldOpenConn.Load() {
			return Opener(testDSN, nil)(ctx)
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
	unblocked.Add(maxWaiters)
	for range maxWaiters {
		go func() {
			defer unblocked.Done()
			_, err := p.Get(ctx)
			if !errors.Is(err, ErrPoolClosed) {
				panic(err)
			}
		}()
	}

	waiters.Wait()

	// Make the next Next call open a connection instead of block.
	shouldOpenConn.Store(true)

	_, err := p.Get(ctx)
	if !errors.Is(err, ErrMaxWaiting) {
		t.Fatalf("Get() = %v; want %v", err, ErrMaxWaiting)
	}
}

func TestPoolGetAllocations(t *testing.T) {
	t.Skip("TODO: unskip and fix after synctest problems are fixed")

	p := &Pool{
		Open: func(context.Context) (*Conn, error) {
			return &Conn{closed: true}, nil
		},

		// Ensure each Get results in a new connection.
		MaxOpen: 1,
		MaxUses: 1,
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

	closedConn := &Conn{closed: true}

	p := &Pool{
		Open: func(context.Context) (*Conn, error) {
			return closedConn, nil
		},

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
		Open: func(context.Context) (*Conn, error) {
			return &Conn{closed: true}, nil
		},

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
	b.Run("driver=pqlite/open=1", func(b *testing.B) { benchmarkPool(b, 1) })
	b.Run("driver=pqlite/open=2", func(b *testing.B) { benchmarkPool(b, 2) })
	b.Run("driver=pqlite/open=4", func(b *testing.B) { benchmarkPool(b, 4) })
	b.Run("driver=pqlite/open=8", func(b *testing.B) { benchmarkPool(b, 8) })
	b.Run("driver=pqlite/open=16", func(b *testing.B) { benchmarkPool(b, 16) })
	b.Run("driver=pqlite/open=32", func(b *testing.B) { benchmarkPool(b, 32) })
	b.Run("driver=pqlite/open=48", func(b *testing.B) { benchmarkPool(b, 48) })
	b.Run("driver=pqlite/open=64", func(b *testing.B) { benchmarkPool(b, 64) })

	b.Run("driver=pgx/open=1", func(b *testing.B) { benchmarkPoolPGX(b, 1) })
	b.Run("driver=pgx/open=2", func(b *testing.B) { benchmarkPoolPGX(b, 2) })
	b.Run("driver=pgx/open=4", func(b *testing.B) { benchmarkPoolPGX(b, 4) })
	b.Run("driver=pgx/open=8", func(b *testing.B) { benchmarkPoolPGX(b, 8) })
	b.Run("driver=pgx/open=16", func(b *testing.B) { benchmarkPoolPGX(b, 16) })
	b.Run("driver=pgx/open=32", func(b *testing.B) { benchmarkPoolPGX(b, 32) })
	b.Run("driver=pgx/open=48", func(b *testing.B) { benchmarkPoolPGX(b, 48) })
	b.Run("driver=pgx/open=64", func(b *testing.B) { benchmarkPoolPGX(b, 64) })
}

func benchmarkPool(b *testing.B, maxOpen int) {
	p := &Pool{
		MaxOpen: maxOpen,
		Open:    Opener(testDSN, nil),
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
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}
	wg.Wait()

	b.ReportMetric(float64(getns.Load())/float64(b.N), "ns/get")
	b.ReportMetric(float64(slices.Max(maxgets)), "ns/getmax")

	b.ReportMetric(float64(putns.Load())/float64(b.N), "ns/put")
	b.ReportMetric(float64(slices.Max(maxputs)), "ns/putmax")
}

func benchmarkPoolPGX(b *testing.B, maxOpen int) {
	dsn := testDSN + " pool_max_conns=" + strconv.Itoa(maxOpen)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()

	var getns atomic.Int64 // time to get
	maxgets := make([]time.Duration, runtime.NumCPU())
	var putns atomic.Int64 // time to put
	maxputs := make([]time.Duration, runtime.NumCPU())

	var rem atomic.Int64
	rem.Store(int64(b.N))
	var wg sync.WaitGroup
	for i := range runtime.NumCPU() {
		wg.Add(1)
		go func() {
			defer wg.Done()

			zzz := rand.NewZipf(rand.New(rand.NewPCG(1, 2)), 2.0, 1.5, 100)
			for rem.Add(-1) >= 0 {
				start := time.Now()
				cn, err := pool.Acquire(ctx)
				if err != nil {
					panic(err)
				}
				d := time.Since(start)
				getns.Add(d.Nanoseconds())
				maxgets[i] = max(maxgets[i], d)

				time.Sleep(time.Duration(zzz.Uint64()) * time.Millisecond)

				start = time.Now()
				cn.Release()
				d = time.Since(start)
				putns.Add(d.Nanoseconds())
				maxputs[i] = max(maxputs[i], d)
			}
		}()
	}
	wg.Wait()

	b.ReportMetric(float64(getns.Load())/float64(b.N), "ns/get")
	b.ReportMetric(float64(slices.Max(maxgets)), "ns/getmax")

	b.ReportMetric(float64(putns.Load())/float64(b.N), "ns/put")
	b.ReportMetric(float64(slices.Max(maxputs)), "ns/putmax")
}
