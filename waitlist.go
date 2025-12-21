package pqlite

import (
	"context"
	"errors"
	"sync"

	"blake.io/pqlite/queues"
)

// Errors
var (
	ErrMaxWaiters = errors.New("too many waiters")
	ErrClosed     = errors.New("closed")
)

type WaitList[T any] struct {
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

	// waitersMu guards waiters (and testHookWaiterCanceled).
	waitersMu sync.Mutex
	waiters   queues.Fifo[chan T] // FIFO queue of waiters

	chanPool sync.Pool // pool for waiter channels

	testHookWaiterCanceled func(ch chan T)
}

// Take blocks until an item is available from the pool or ctx is done,
// which ever comes first.
//
// If no items are available, and the number of loaded items is less than
// MaxItems, or MaxItems is zero, it calls the provided load function in a new
// goroutine to create a new item, which will be handed out to the next waiter,
// not neccessarily the caller that provided the load function. This ensures
// that items are handed out in FIFO order.
//
// If no items are available, and the WaitList is closed, it returns ErrClosed
// immediately and does not block waiting for another item.
func (p *WaitList[T]) Take(ctx context.Context, load func() T) (T, error) {
	var zero T
	if ctx.Err() != nil {
		return zero, ctx.Err()
	}

	p.readyMu.Lock()
	if v, ok := p.ready.Pop(); ok {
		p.readyMu.Unlock()
		return v, nil
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

	if p.MaxItems == 0 || p.loads < p.MaxItems {
		p.loads++
		go func() {
			defer func() {
				if r := recover(); r != nil {
					// load panicked; decrement loads so pool can recover
					p.readyMu.Lock()
					p.loads--
					p.readyMu.Unlock()
					panic(r) // re-panic after cleanup
				}
			}()
			var ready T
			if load != nil {
				ready = load()
			}
			p.Put(ready)
		}()
	}

	p.waitersMu.Unlock()
	p.readyMu.Unlock()

	select {
	case v := <-ch:
		p.chanPool.Put(ch)
		return v, nil
	case <-ctx.Done():
		if p.testHookWaiterCanceled != nil {
			p.testHookWaiterCanceled(ch)
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

		return zero, ctx.Err()
	}
}

// TryTake pops the next ready item without blocking.
// It returns the item and ok=true when something is available.
// If the ready queue is empty it returns the zero value and ok=false and does
// not enqueue the caller as a waiter.
func (p *WaitList[T]) TryTake() (_ T, ok bool) {
	p.readyMu.Lock()
	defer p.readyMu.Unlock()
	return p.ready.Pop()
}

// Put hands v to the longest waiting waiter, or adds it to the ready list for
// the next call to Take.
//
// If the WaitList is closed, Put does nothing.
func (p *WaitList[T]) Put(v T) {
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

// shouldLoad returns true if a new item should be loaded.
// If true, the caller must call Put() with the loaded item, or cancelLoad() if loading fails.
func (p *WaitList[T]) shouldLoad() bool {
	p.readyMu.Lock()
	defer p.readyMu.Unlock()

	if p.MaxItems == 0 || p.loads < p.MaxItems {
		p.loads++
		return true
	}
	return false
}

// cancelLoad decrements the loads counter when a load operation fails.
func (p *WaitList[T]) cancelLoad() {
	p.readyMu.Lock()
	p.loads--
	p.readyMu.Unlock()
}

// Wait blocks until an item is available or ctx is done.
// Unlike Take, it does not trigger loading - the caller must handle that.
func (p *WaitList[T]) Wait(ctx context.Context) (T, error) {
	var zero T
	if ctx.Err() != nil {
		return zero, ctx.Err()
	}

	p.readyMu.Lock()
	if v, ok := p.ready.Pop(); ok {
		p.readyMu.Unlock()
		return v, nil
	}

	p.waitersMu.Lock()
	if p.MaxWaiters > 0 && p.waiters.Len() >= p.MaxWaiters {
		p.waitersMu.Unlock()
		p.readyMu.Unlock()
		return zero, ErrMaxWaiters
	}

	// Get in line and wait
	ch, _ := p.chanPool.Get().(chan T)
	if ch == nil {
		ch = make(chan T, 1)
	}
	p.waiters.Unshift(ch)

	p.waitersMu.Unlock()
	p.readyMu.Unlock()

	select {
	case v := <-ch:
		p.chanPool.Put(ch)
		return v, nil
	case <-ctx.Done():
		if p.testHookWaiterCanceled != nil {
			p.testHookWaiterCanceled(ch)
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

		return zero, ctx.Err()
	}
}
