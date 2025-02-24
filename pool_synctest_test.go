package pqlite

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

func TestPoolSyncExpired(t *testing.T) {
	synctest.Run(func() {
		p := &Pool{
			MaxLifetime: 3 * time.Second,
			Logger:      testutil.Slogger(t),
			Open:        Opener(testDSN, nil),
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
	synctest.Run(func() {
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
		var g sync.WaitGroup
		for i := range 3 {
			g.Add(1)
			go func() {
				defer g.Done()
				cn, err := p.Get(ctx)
				got[i] = err
				if err == nil {
					panic("expected error")
				}
				if cn != nil {
					cn.Close()
					panic("expected nil conn")
				}
			}()
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

	synctest.Run(func() {
		p := &Pool{
			Logger:  testutil.Slogger(t),
			Open:    closedConnOpener,
			MaxOpen: 1,
		}
		defer p.Close()
		defer p.checkAndDrainPool()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Acquire the only allowed Conn in the pool
		want, err := p.Get(ctx)
		check(err)

		p.testHookWaitCanceled = func(ch chan *Conn) {
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

	synctest.Run(func() {
		p := &Pool{
			Logger:  testutil.Slogger(t),
			Open:    closedConnOpener,
			MaxOpen: 1,
		}
		defer p.Close()
		defer p.checkAndDrainPool()

		// Acquire the only allowed Conn in the pool
		_, err := p.Get(t.Context())
		check(err)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		p.testHookWaitCanceled = func(ch chan *Conn) {
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

		// Run until Get reaches the select; without this we risk
		// getting a false positive if the initial context error check
		// in Get is scheduled after we cancel the context.
		synctest.Wait()

		// cancel the context causing it to fall into the <-ctx.Done() case
		cancel()

		got := <-ch
		if errors.Is(got, ErrPoolClosed) {
			// special case check to callout this test is to ensure
			// we never get ErrPoolClosed in particular.
			t.Fatal("unexpected pool closed error")
		}
		if !errors.Is(got, context.Canceled) {
			t.Fatalf("err = %v; want %v", got, context.Canceled)
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

func closedConnOpener(context.Context) (*Conn, error) {
	return &Conn{closed: true}, nil
}

func sleepUntilDeadline(ctx context.Context) {
	d, ok := ctx.Deadline()
	if !ok {
		panic("no deadline")
	}
	time.Sleep(time.Until(d))
}
