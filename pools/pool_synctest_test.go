package pools

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"blake.io/pqlite/internal/testutil"
)

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
