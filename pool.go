package polite

import (
	"cmp"
	"context"
	"errors"
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

// Pool is a pool of connections. It is safe for concurrent use by multiple
// goroutines.
//
// Concurrent [Pool.Get] callers are served in FIFO order to avoid starvation.
// Each waiter receives the most recently returned connection, preserving warm
// per-connection state such as prepared statements.
//
// The zero value is ready for use and uses [DefaultPoolMaxWaiters] as the
// waiting limit.
type Pool struct {
	// Dial is a function that is called to open a new connection when
	// the pool is empty.
	//
	// If Dial is nil then the package-level Dial function is used with
	// [DefaultDSN] and nil config.
	Dial func(ctx context.Context) (*Conn, error)

	// MaxItems is the maximum number of connections that can be open at
	// once. If MaxItems is zero, then [DefaultPoolMaxConns] is used.
	MaxItems int

	// MaxWaiters is the maximum number of waiters that can be waiting
	// for a connection. If this number is exceeded, Get will return an
	// error. If MaxWaiters is zero, then [DefaultPoolMaxWaiters] is used.
	MaxWaiters int

	// Logger logs pool activity. If Logger is nil, slog.Default() is used.
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
// Put closes cn instead.
//
// A connection is idle when it has no error, no in-progress COPY, and
// its transaction status is [febe.ReadyStatusIdle]. Callers that leave
// a connection in any other state (open transaction, failed
// transaction, mid-COPY) must clean it up before calling Put, or
// accept that Put will discard it.
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
// If ctx is canceled or times out while waiting, Get returns an error that
// wraps [context.Canceled] or [context.DeadlineExceeded]. No cancel request is
// sent to PostgreSQL because no operation has started on a connection.
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

// Close closes the pool and its idle connections. In-use connections are
// left to the caller. After Close, Get returns [ErrClosed]; waiters
// already blocked in Get wake with the same error. Put closes any
// connection handed back.
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
	for i := range backoff.Loop(p.cancelCtx, 5*time.Second) {
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
