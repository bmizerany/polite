package backoff

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestLoopAllocs(t *testing.T) {
	for i := range 3 {
		got := testing.AllocsPerRun(1000, func() {
			for tick := range Loop(t.Context(), 1) {
				if tick >= i {
					break
				}
			}
		})
		want := float64(0)
		if i > 0 {
			want = 3 // due to time.NewTimer
		}
		if got > want {
			t.Errorf("[%d ticks]: allocs = %v, want 0", i, want)
		}
	}
}

func TestLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		last := -1

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		for n, err := range Loop(ctx, 100*time.Millisecond) {
			if !errors.Is(err, ctx.Err()) {
				t.Errorf("err = %v, want nil", err)
			}
			if err != nil {
				break
			}
			if n != last+1 {
				t.Errorf("n = %d, want %d", n, last+1)
			}
			last = n
			if n > 5 {
				cancel()
			}
		}

		if last != 6 {
			t.Errorf("last = %d, want 6", last)
		}
	})
}
