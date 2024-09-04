//Xgo:build synctest

package pqlite

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/neilotoole/slogt"
)

func TestPoolGetNextError(t *testing.T) {
	synctest.Run(func() {
		var retries atomic.Int64

		p := &Pool{
			Logger: slogt.New(t),
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
