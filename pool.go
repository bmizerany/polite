package polite

import (
	"cmp"
	"context"
	"errors"
	"iter"
	"log/slog"
	"sync"
	"time"

	"blake.io/polite/internal/backoff"
	"blake.io/wait"
)

// Defaults
const (
	DefaultPoolMaxConns   = 100 // Postgres default
	DefaultPoolMaxWaiters = 1000

	DefaultDSN = "dbname=postgres host=localhost port=5432 sslmode=verify-full"
)

// Pool is a pool of connections, safe for concurrent use by multiple
// goroutines. See the package documentation for the scheduling discipline.
type Pool struct {
	// Dial opens a new connection when the pool needs one. If Dial is
	// nil, the package-level Dial function is used with DefaultDSN and
	// nil config.
	//
	// Dial is invoked with the pool's lifetime context, not the
	// caller's Get context, so canceling a Get does not cancel
	// an in-flight dial.
	//
	// Errors returned by Dial trigger an exponential-backoff retry
	// (sleep capped at 5s) that continues until Close. Permanent
	// errors such as bad credentials, an unknown database, or TLS
	// failures are not surfaced to callers and will retry forever;
	// they are logged through the Logger field. Callers cancel by
	// closing the pool.
	Dial func(ctx context.Context) (*Conn, error)

	// MaxItems is the maximum number of connections that can be open at
	// once. If MaxItems is zero, then DefaultPoolMaxConns is used.
	MaxItems int

	// MaxWaiters is the maximum number of waiters that can be waiting
	// for a connection. If this number is exceeded, Get returns
	// ErrTooManyWaiters. If MaxWaiters is zero, then
	// DefaultPoolMaxWaiters is used.
	MaxWaiters int

	// Logger receives the pool's "error opening new connection;
	// retrying" warnings emitted by the Dial field's retry loop. If
	// Logger is nil, slog.Default is used. To silence these
	// warnings, pass a logger backed by slog.DiscardHandler.
	Logger *slog.Logger

	waitlist  wait.List[*Conn]
	initOnce  sync.Once
	cancelCtx context.Context
	cancel    context.CancelCauseFunc
}

func (p *Pool) init() {
	p.initOnce.Do(func() {
		p.cancelCtx, p.cancel = context.WithCancelCause(context.Background())
		p.waitlist.MaxItems = p.maxItems()
		p.waitlist.MaxWaiters = p.maxWaiters()
		p.waitlist.New = p.openConn
	})
}

// Put returns cn to the pool. If the pool is closed or cn is not idle,
// Put closes cn instead. A connection returned in a non-ready state
// is retired, freeing its slot so the next [Pool.Get] may dial a
// fresh connection.
//
// A connection is idle when its transport is healthy, no COPY is in
// progress, and the backend is at ReadyForQuery outside any
// transaction block. Callers that leave a connection in any other
// state must clean it up before calling Put, or accept that Put will
// close and discard it.
func (p *Pool) Put(cn *Conn) {
	p.init()
	if p.cancelCtx.Err() != nil || !cn.Idle() {
		cn.Close()
		p.waitlist.Retire()
		return
	}
	p.waitlist.Put(cn)
}

// Get returns a connection from the pool.
//
// Get returns [ErrTooManyWaiters] when the waiting limit is exceeded
// and [ErrClosed] when the pool is closed (including waiters woken by
// a concurrent [Pool.Close]). If ctx is canceled or times out while
// waiting, Get returns an error that wraps [context.Canceled] or
// [context.DeadlineExceeded]. No cancel request is sent to PostgreSQL
// because no operation has started on a connection.
//
// Canceling a Get wait does not cancel an in-flight open attempt started for
// that wait. If the open later succeeds, a later caller to [Pool.Get] may
// receive that connection.
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	if ctx.Err() != nil {
		// avoid tying up resources and delaying a connection from
		// reaching a truly ready waiter if the context is already
		// done.
		return nil, ctx.Err()
	}

	p.init()
	if p.cancelCtx.Err() != nil {
		return nil, cmp.Or(context.Cause(p.cancelCtx), ErrClosed)
	}

	cn, err := p.waitlist.Take(ctx)
	switch {
	case errors.Is(err, wait.ErrMaxWaiters):
		return nil, ErrTooManyWaiters
	case errors.Is(err, wait.ErrClosed):
		// Pool was closed while waiting
		return nil, cmp.Or(context.Cause(p.cancelCtx), ErrClosed)
	default:
		return cn, err
	}
}

// Exec checks a connection out of the pool, runs sql, and returns the
// connection to the pool. It is the [Pool] analogue of [Conn.Exec] for
// the common one-shot case where the caller does not need to thread a
// [*Conn] through several calls.
//
// If the pool cannot supply a connection, Exec returns the error from
// [Pool.Get] without running sql. Otherwise it returns whatever
// [Conn.Exec] returns. The connection is always returned to the pool;
// [Pool.Put] decides whether the connection is still healthy enough to
// keep.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) error {
	cn, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Put(cn)
	return cn.Exec(ctx, sql, args...)
}

// Query checks a connection out of the pool, runs sql, and returns the
// connection to the pool when the iterator finishes. It is the [Pool]
// analogue of [Conn.Query]; the iterator owns the connection for its
// lifetime, so callers must let the loop run to completion (or break)
// before issuing another pool operation that depends on the released
// connection.
//
// If [Pool.Get] fails, Query yields a single error [Row] carrying that
// error and stops. Errors from [Conn.Query] are surfaced through
// columns the same way [Conn.Query] surfaces them; see that method
// for the iteration contract.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) iter.Seq[*Row] {
	return func(yield func(*Row) bool) {
		cn, err := p.Get(ctx)
		if err != nil {
			yield(&Row{err: err})
			return
		}
		defer p.Put(cn)
		for row := range cn.Query(ctx, sql, args...) {
			if !yield(row) {
				return
			}
		}
	}
}

// Close closes the pool and every idle connection in it. After Close,
// [Pool.Get] returns [ErrClosed], and waiters already blocked in Get
// wake with the same error. Connections checked out via Get keep
// running and are the caller's to use; [Pool.Put] closes them when
// they come back rather than returning them to the pool.
//
// Close does not wait for in-flight operations, checked-out
// connections, or the [Pool.Dial] retry loop to unwind, and returns
// once it has signaled the closure and drained the idle set. The first
// call returns nil; subsequent calls return [context.Canceled].
//
// As a known limitation, an open attempt already in flight when Close
// runs may complete after Close returns and leak its connection,
// since Close does not wait for the [Pool.Dial] retry loop to unwind.
func (p *Pool) Close() error {
	p.init()
	if p.cancelCtx.Err() != nil {
		return p.cancelCtx.Err()
	}
	p.cancel(ErrClosed)

	// Close the waitlist to wake up all waiters
	p.waitlist.Close()

	// Drain the waitlist by taking and closing all ready connections
	for {
		cn, ok := p.waitlist.TryTake()
		if !ok {
			break
		}
		if cn != nil {
			cn.Close()
		}
	}

	// TODO(bmizerany): wait for all connections to be returned? We should
	// consider taking a context if so, we don't want to block forever if
	// someone is holding a connection or accidentally forgets to return
	// theirs back to the pool.

	return nil
}

func (p *Pool) openConn() *Conn {
	for i := range backoff.Loop(p.cancelCtx, 3*time.Second) {
		cn, err := p.open(p.cancelCtx)
		if err == nil {
			return cn
		}
		if p.cancelCtx.Err() != nil {
			return nil
		}
		if i > 3 {
			p.log().Info("polite: pool: error opening new connection; retrying",
				"err", cmp.Or(context.Cause(p.cancelCtx), err))
		}
	}
	return nil
}

func (p *Pool) open(ctx context.Context) (*Conn, error) {
	if p.Dial != nil {
		return p.Dial(ctx)
	}
	return Dial(ctx, DefaultDSN, nil)
}

func (p *Pool) maxItems() int {
	if p.MaxItems == 0 {
		return DefaultPoolMaxConns
	}
	return p.MaxItems
}

func (p *Pool) maxWaiters() int {
	if p.MaxWaiters == 0 {
		return DefaultPoolMaxWaiters
	}
	return p.MaxWaiters
}

func (p *Pool) log() *slog.Logger {
	if p.Logger == nil {
		return slog.Default()
	}
	return p.Logger
}
