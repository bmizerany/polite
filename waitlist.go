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
	// It also guards waitersMu (see below).
	readyMu   sync.Mutex
	ready     queues.List[T]      // LIFO list of ready items (PushFront+PopFront)
	loads     int                 // number of items loaded in all calls to Take
	readyFree []*queues.Handle[T] // reusable ready list handles

	// waitersMu guards waiters (and testHookWaiterCanceled).
	waitersMu sync.Mutex
	waiters   queues.List[chan T] // FIFO list of waiters (PushFront+PopBack)

	freeWaitChans queues.Fifo[chan T] // reusable waiter channels

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
	if node := p.ready.PopFront(); node != nil {
		v := node.Value()
		p.recycleReady(node)
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
	ch, _ := p.freeWaitChans.Shift()
	if ch == nil {
		ch = make(chan T, 1)
	}
	waiterHandle := p.waiters.PushFront(nil, ch)

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
		return v, nil
	case <-ctx.Done():
		if p.testHookWaiterCanceled != nil {
			p.testHookWaiterCanceled(ch)
		}

		p.waitersMu.Lock()
		p.waiters.Unlink(waiterHandle)
		p.freeWaitChans.Unshift(ch)
		p.waitersMu.Unlock()

		select {
		case v := <-ch:
			// near miss
			return v, nil
		default:
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
	node := p.ready.PopFront()
	if node != nil {
		v := node.Value()
		p.recycleReady(node)
		return v, true
	}
	var zero T
	return zero, false
}

// Put hands v to the longest waiting waiter, or adds it to the ready list for
// the next call to Take.
//
// If the WaitList is closed, Put does nothing.
func (p *WaitList[T]) Put(v T) {
	p.readyMu.Lock()
	p.waitersMu.Lock()
	waiter := p.waiters.PopBack()
	if waiter != nil {
		ch := waiter.Value()
		select {
		case ch <- v:
		default:
			panic("waiter: waiter channel full (this is a bug in waitlist)")
		}
		p.waitersMu.Unlock()
		p.readyMu.Unlock()
		return
	}
	p.ready.PushFront(p.getReadyHandle(), v)
	p.waitersMu.Unlock()
	p.readyMu.Unlock()
}

func (p *WaitList[T]) getReadyHandle() *queues.Handle[T] {
	if n := len(p.readyFree); n > 0 {
		h := p.readyFree[n-1]
		p.readyFree = p.readyFree[:n-1]
		return h
	}
	return new(queues.Handle[T])
}

func (p *WaitList[T]) recycleReady(h *queues.Handle[T]) {
	*h = queues.Handle[T]{}
	p.readyFree = append(p.readyFree, h)
}
