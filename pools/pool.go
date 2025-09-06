package pools

import (
	"cmp"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"blake.io/pqlite/backoff"
	"blake.io/pqlite/queues"
)

// Defaults
const (
	DefaultPoolMaxConns    = 100 // Postgres default
	DefaultPoolMaxWaiters  = 1000
	DefaultPoolMaxUses     = 1000
	DefaultPoolMaxLifetime = 5 * time.Minute

	DefaultDSN = "dbname=postgres host=localhost port=5432 sslmode=disable"
)

type Conn struct {
	uses    int
	created time.Time
	closed  atomic.Bool
}

func (c *Conn) Close() error {
	if c.closed.Swap(true) {
		return net.ErrClosed
	}
	return nil
}

// Errors
var (
	ErrMaxWaiting = errors.New(" pool: too many waiting")
	ErrPoolClosed = errors.New(" pool: closed")
)

// Pool is a pool of values. It is safe for concurrent use by multiple
// goroutines. The zero value is a pool with no limit on the number of
// waiters, ready for use.
type Pool[T io.Closer] struct {
	// Open is a function that is called to open a new connection when
	// the pool is empty.
	//
	// If Open is nil then DefaultOpener is used.
	Open func(ctx context.Context) (T, error)

	// MaxConns is the maximum number of cvalues that can be open at
	// once. If MaxConns is zero, then DefaultPoolMaxConns is used.
	MaxOpen int

	// MaxUses is the maximum number of times a connection can be used
	// before it is closed. If MaxUses is zero, then [DefaultPoolMaxUses] is
	// used.
	MaxUses int

	// MaxWaiting is the maximum number of waiters that can be waiting
	// for a connection. If this number is exceeded, Get will return an
	// error. If MaxWaiting is zero, then [DefaultPoolMaxWaiters] is used.
	MaxWaiting int

	// MaxLifetime is the maximum amount of time a connection can be
	// idle before it is closed. If MaxLifetime is zero, then
	// DefaultPoolMaxLifetime is used.
	MaxLifetime time.Duration

	Logger *slog.Logger // default is slog.Default()

	// TODO(bmizerany): MaxLifetime time.Duration

	readyMu   sync.Mutex // guards the following fields
	ready     queues.Lifo[T]
	openConns int
	cancelCtx context.Context
	cancel    context.CancelCauseFunc

	connChanPool sync.Pool

	waitingMu sync.Mutex
	waiting   queues.Fifo[chan T]

	// testHookWaiterCanceled, if set, is called from Get when the wait is
	// canceled. It is not used by pqlite code itself - it is only here for use
	// in tests.
	//
	// The channel passed to testHookWaiterCanceled is the channel for the waiter.
	//
	// It is called before ch is removed from the wait queue and before it is
	// checked for a near miss.
	testHookWaiterCanceled func(ch chan T)
}

func (p *Pool[T]) putConnChan(ch chan T) {
	select {
	case _, ok := <-ch:
		if !ok {
			panic(" pool: unexpected closed channel in putConnChan")
		}
		panic(" pool: unexpected non-empty channel in putConnChan")
	default:
		p.connChanPool.Put(ch)
	}
}

// caller must hold p.readyMu and p.waitingMu
func (p *Pool[T]) putLocked(cn T) {
	handoff := func(cn T) bool {
		ch, ok := p.waiting.Shift()
		if ok {
			// Hand the connection to the first waiter in line. We
			// guarantee nothing is added to the waiting queue that does
			// not immediately start waiting.
			ch <- cn
			return true
		}
		return false
	}

	if handoff(cn) {
		return
	}

	// TODO(bmizerany): It is currently possible for a client to add a
	// connection to the pool that was not started by the pool. This
	// leads to our openConns count being off. We should consider
	// adding a flag to the connection to indicate if it was started by
	// the pool or not. This would also allow us to avoid the
	// connection being closed by the pool if it was not started by the
	// pool.
	p.ready.Push(cn)
}

func (p *Pool[T]) put(cn T) {
	p.readyMu.Lock()
	if p.cancelCtx.Err() != nil {
		p.readyMu.Unlock()
		cn.Close()
		return
	}
	p.waitingMu.Lock()
	p.putLocked(cn)
	p.waitingMu.Unlock()
	p.readyMu.Unlock()
}

// Put adds a connection back to the pool. Put should be called when
// finished with a connection to return it to the pool. If the Conn is
// closed, nothing happens. If the pool is closed, the connection is also
// closed.
//
// Put is a non-blocking operation.
func (p *Pool[T]) Put(cn T) {
	expired := p.MaxLifetime > 0 && time.Since(cn.created) >= p.MaxLifetime
	cn.uses++
	if expired || p.maxUses() > 0 && cn.uses >= p.maxUses() {
		go p.startNewConn(cn)
	} else {
		p.put(cn)
	}
}

func (p *Pool[T]) startNewConn(retire T) {
	if retire != nil {
		// Retire the connection so that it is not used again, and
		// before we open a new one as to not exceed the max open
		// values.
		//
		// This is done in a goroutine to avoid blocking callers of Put
		// that would otherwise unjustly pay the penalty of the
		// connection being retired.
		retire.Close()
	}

	for i, err := range backoff.Loop(p.cancelCtx, 5*time.Second) {
		if err != nil {
			// Poll is closed.
			return
		}
		cn, err := p.Open(p.cancelCtx)
		if err == nil {
			cn.created = time.Now()
			p.put(cn)
			return
		}
		if i > 3 {
			p.log().Info(" pool: error opening new connection; retrying", "err",
				cmp.Or(context.Cause(p.cancelCtx), err))
		}
	}
}

func (p *Pool[T]) dropWaiter(ch chan T) {
	p.readyMu.Lock()
	p.waitingMu.Lock()
	p.waiting.DeleteFunc(func(dch chan T) bool {
		return ch == dch
	})
	p.waitingMu.Unlock()
	p.readyMu.Unlock()
}

func (p *Pool[T]) Get(ctx context.Context) (T, error) {
	if ctx.Err() != nil {
		// avoid tying up resources and delaying a connection from
		// reaching a truly ready waiter if the context is already
		// done.
		return nil, ctx.Err()
	}
	cn, ch, err := func() (T, chan T, error) {
		p.readyMu.Lock()
		if p.cancelCtx == nil {
			p.cancelCtx, p.cancel = context.WithCancelCause(context.Background())
		}
		if p.cancelCtx.Err() != nil {
			p.readyMu.Unlock()
			return nil, nil, p.cancelCtx.Err()
		}
		cn, ok := p.ready.Pop()
		if ok {
			p.readyMu.Unlock()
			return cn, nil, nil
		}

		p.waitingMu.Lock()
		if p.waiting.Len() >= p.maxWaiting() {
			p.waitingMu.Unlock()
			p.readyMu.Unlock()
			return nil, nil, ErrMaxWaiting
		}

		// Get in line and wait for another goroutine to put a
		// connection in the pool or for any new connection being
		// opened to be handed off to us.
		ch, _ := p.connChanPool.Get().(chan T)
		if ch == nil {
			ch = make(chan T, 1)
		}
		p.waiting.Unshift(ch)
		p.waitingMu.Unlock()

		if p.openConns < p.maxOpen() || p.maxOpen() < 0 {
			p.openConns++
			go p.startNewConn(nil)
		}
		p.readyMu.Unlock()
		return nil, ch, nil
	}()
	if err != nil {
		return nil, cmp.Or(context.Cause(p.cancelCtx), err)
	}

	// A ready connection was obtained, so return it.
	if cn != nil {
		return cn, nil
	}

	// We're in line and waiting for a connection to be handed off to us,
	// so wait.

	select {
	case cn, ok := <-ch:
		if !ok {
			return nil, ErrPoolClosed
		}
		p.connChanPool.Put(ch)
		return cn, nil
	case <-ctx.Done():
		if p.testHookWaiterCanceled != nil {
			p.testHookWaiterCanceled(ch)
		}

		select {
		case cn, ok := <-ch:
			if !ok {
				// Prefer context cancellation over pool
				// closure errors.
				//
				// A context error suggests a temporary
				// condition - perhaps a deadline or timeout -
				// from which the client can recover. By
				// contrast, a closed pool signals a more
				// fundamental issue that might discourage the
				// client from attempting further work.
				//
				// We choose to preserve opportunities for
				// recovery by surfacing the context error,
				// letting clients make informed decisions
				// about retrying operations.
				return nil, ctx.Err()
			}

			p.dropWaiter(ch)

			// Handle successful connection acquisition despite
			// context cancellation
			//
			// If we obtain a connection just as the context
			// expires, we treat this as a "soft cancellation" -
			// returning the connection instead of an error.
			//
			// This approach:
			//
			//   1. Preserves work already done in acquiring the connection
			//   2. Lets callers implement local timeout boundaries
			//
			// Example of local timeout boundary:
			//
			//     cn, err := func() {
			//         ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond)
			//         return pool.Get(ctx)
			//     }()
			//     ... use cn ...
			//
			// The timeout above only constrains connection
			// acquisition, not the connection's subsequent usage.
			// This pattern gives callers fine-grained control over
			// timeout behaviors.
			//
			// We delegate connection cleanup to the caller rather
			// than handling it here, as they can best determine
			// whether to treat the cancellation as hard (requiring
			// cleanup) or soft (allowing continued use).
			p.connChanPool.Put(ch)
			return cn, nil
		default:
			p.connChanPool.Put(ch)
			return nil, ctx.Err()
		}
	case <-p.cancelCtx.Done():
		// Pool closed while we were waiting.
		p.dropWaiter(ch)
		select {
		case cn, ok := <-ch:
			// near miss (see above for rationale)
			if ok {
				// If the pool is closed,
				// drop the value on the floor.
				cn.Close()
			}
		default:
		}
		p.connChanPool.Put(ch)
		return nil, ErrPoolClosed
	}
}

// CloseExpiredConns closes any values that have been idle for more than
// MaxLifetime.
//
// It returns all errors encountered while closing values wrapped into
// one.
func (p *Pool[T]) CloseExpiredConns() error {
	maxLifetime := p.MaxLifetime
	if maxLifetime == 0 {
		maxLifetime = DefaultPoolMaxLifetime
	}

	var errs []error
	var marked []T

	p.readyMu.Lock()
	p.ready.DeleteFunc(func(cn T) bool {
		if time.Since(cn.created) > p.MaxLifetime {
			marked = append(marked, cn)
			return true
		}
		return false
	})
	p.readyMu.Unlock()

	for _, cn := range marked {
		if err := cn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *Pool[T]) Close() error {
	p.readyMu.Lock()
	defer p.readyMu.Unlock()
	return p.closeReadyLocked()
}

func (p *Pool[T]) closeReadyLocked() error {
	if p.cancelCtx == nil {
		p.cancelCtx, p.cancel = context.WithCancelCause(context.Background())
	}

	if p.cancelCtx.Err() != nil {
		return p.cancelCtx.Err()
	}

	// unblock all waiters and prevent new ones
	p.cancel(ErrPoolClosed)

	// Next, close all ready values followed by all waiting
	// values.
	p.waitingMu.Lock()
	defer p.waitingMu.Unlock()

	for {
		cn, ok := p.ready.Pop()
		if !ok {
			break
		}
		cn.Close()
	}

	// TODO(bmizerany): wait for all values to be returned? We should
	// consider taking a context if so, we don't want to block forever if
	// someone is holding a connection or accidentally forgets to return
	// theirs back to the pool.

	return nil
}

func (p *Pool[T]) maxOpen() int {
	if p.MaxOpen == 0 {
		return DefaultPoolMaxConns
	}
	return p.MaxOpen
}

func (p *Pool[T]) maxUses() int {
	if p.MaxUses == 0 {
		return DefaultPoolMaxUses
	}
	return p.MaxUses
}

func (p *Pool[T]) maxWaiting() int {
	if p.MaxWaiting == 0 {
		return DefaultPoolMaxWaiters
	}
	return p.MaxWaiting
}

func (p *Pool[T]) log() *slog.Logger {
	if p.Logger == nil {
		return slog.Default()
	}
	return p.Logger
}
