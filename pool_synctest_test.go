//go:build synctest

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

func TestPoolExpired(t *testing.T) {
	synctest.Run(func() {
		p := &Pool{
			MaxLifetime: 3 * time.Second,
			Logger:      testutil.Slogger(t),
			Next:        Opener(testDSN, nil),
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

func TestPoolGetNextError(t *testing.T) {
	synctest.Run(func() {
		var retries atomic.Int64

		p := &Pool{
			Logger: testutil.Slogger(t),
			Next: func(context.Context) (*Conn, error) {
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

		for i := range 3 {
			if got[i] == nil {
				t.Fatalf("[%d]: unexpected nil error", i)
			}
		}

		if retries.Load() <= 3 {
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
