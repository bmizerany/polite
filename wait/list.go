package wait

import (
	"context"
	"errors"
	"sync"

	"blake.io/pqlite/internal/queues"
)

// Errors
var (
	ErrMaxWaiters = errors.New("too many waiters")
	ErrClosed     = errors.New("closed")
)

type List[T any] struct {
	// MaxItems is the maximum number of items that can be loaded into the
	// pool. If zero, there is no limit and callers must ensure load functions
	// are bounded to avoid unbounded resource growth. Must be non-negative.
	MaxItems int

	// MaxWaiters is the maximum number of goroutines that can wait for an
	// item. Take returns ErrMaxWaiters if this limit is reached. If zero,
	// there is no limit on waiters. Must be non-negative.
	MaxWaiters int

	// readyMu guards the ready list and loads counter.
	readyMu sync.Mutex
	ready   queues.Lifo[T] // LIFO stack of ready items
	loads   int            // number of items loaded in all calls to Take

	// waitersMu guards waiters (and TestHookWaiterCanceled).
	waitersMu sync.Mutex
	waiters   queues.Fifo[chan T] // FIFO queue of waiters

	chanPool sync.Pool // pool for waiter channels

	closedOnce sync.Once
	closedCh   chan struct{} // closed when List is closed

	TestHookWaiterCanceled func(ch chan T)
}

func (p *List[T]) init() {
	if p.closedCh == nil {
		p.closedCh = make(chan struct{})
	}
}

// Close closes the List, causing all current and future Wait calls
// to return ErrClosed.
func (p *List[T]) Close() {
	p.init()
	p.closedOnce.Do(func() {
		close(p.closedCh)
	})
}

// Take blocks until an item is available from the pool or ctx is done,
// whichever comes first.
//
// If no items are available and a load function is provided, Take starts the
// load in a new goroutine when MaxItems allows. When load returns an error,
// the load slot is released and the waiter continues to wait.
//
// If no items are available and the List is closed, it returns ErrClosed
// immediately and does not block waiting for another item.
func (p *List[T]) Take(ctx context.Context, load func() (T, error)) (T, error) {
	var zero T
	p.init()
	if ctx.Err() != nil {
		return zero, ctx.Err()
	}

	p.readyMu.Lock()
	if v, ok := p.ready.Pop(); ok {
		p.readyMu.Unlock()
		return v, nil
	}

	select {
	case <-p.closedCh:
		p.readyMu.Unlock()
		return zero, ErrClosed
	default:
	}

	p.waitersMu.Lock()
	if p.MaxWaiters > 0 && p.waiters.Len() >= p.MaxWaiters {
		p.waitersMu.Unlock()
		p.readyMu.Unlock()
		return zero, ErrMaxWaiters
	}

	// Get in line before we start any loading to ensure FIFO order.
	ch, _ := p.chanPool.Get().(chan T)
	if ch == nil {
		ch = make(chan T, 1)
	}
	p.waiters.Unshift(ch)

	startLoad := load != nil && p.shouldLoadLocked()

	p.waitersMu.Unlock()
	p.readyMu.Unlock()

	if startLoad {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.cancelLoad()
					panic(r) // re-panic after cleanup
				}
			}()
			ready, err := load()
			if err != nil {
				p.cancelLoad()
				return
			}
			p.Put(ready)
		}()
	}

	select {
	case v := <-ch:
		p.chanPool.Put(ch)
		return v, nil
	case <-ctx.Done():
		return p.handleCancel(ch, ctx.Err())
	case <-p.closedCh:
		return p.handleCancel(ch, ErrClosed)
	}
}

// TryTake pops the next ready item without blocking.
// It returns the item and ok=true when something is available.
// If the ready queue is empty it returns the zero value and ok=false and does
// not enqueue the caller as a waiter.
func (p *List[T]) TryTake() (_ T, ok bool) {
	p.readyMu.Lock()
	defer p.readyMu.Unlock()
	return p.ready.Pop()
}

// Put hands v to the longest waiting waiter, or adds it to the ready list for
// the next call to Take.
//
// If the List is closed, Put does nothing.
func (p *List[T]) Put(v T) {
	p.init()

	// Check if closed
	select {
	case <-p.closedCh:
		return
	default:
	}

	p.readyMu.Lock()
	p.waitersMu.Lock()
	ch, ok := p.waiters.Shift()
	if ok {
		select {
		case ch <- v:
		default:
			panic("waiter: waiter channel full (this is a bug in waitlist)")
		}
		p.waitersMu.Unlock()
		p.readyMu.Unlock()
		return
	}
	p.ready.Push(v)
	p.waitersMu.Unlock()
	p.readyMu.Unlock()
}

func (p *List[T]) shouldLoadLocked() bool {
	if p.MaxItems == 0 || p.loads < p.MaxItems {
		p.loads++
		return true
	}
	return false
}

func (p *List[T]) cancelLoad() {
	p.readyMu.Lock()
	if p.loads > 0 {
		p.loads--
	}
	p.readyMu.Unlock()
}

func (p *List[T]) handleCancel(ch chan T, err error) (T, error) {
	var zero T

	if p.TestHookWaiterCanceled != nil {
		p.TestHookWaiterCanceled(ch)
	}

	p.waitersMu.Lock()
	p.waiters.DeleteFunc(func(wch chan T) bool {
		return ch == wch
	})
	p.waitersMu.Unlock()

	select {
	case v := <-ch:
		// near miss
		p.chanPool.Put(ch)
		return v, nil
	default:
		p.chanPool.Put(ch)
	}

	return zero, err
}
