package pqlite

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"blake.io/pqlite/internal/testutil"
)

func TestPoolSyncExpired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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

func (p *Pool) checkAndDrainPool() {
	// With WaitList, channel pooling is internal, so this is a no-op
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
