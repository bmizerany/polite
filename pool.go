package polite

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"blake.io/polite/backoff"
	"blake.io/wait"
)

// Defaults
const (
	DefaultPoolMaxConns   = 100 // Postgres default
	DefaultPoolMaxWaiters = 1000

	DefaultDSN = "dbname=postgres host=localhost port=5432 sslmode=verify-full"
)

// Errors
var (
	ErrPoolFull = errors.New("polite: pool: too many waiting")

	// ErrPoolClosed is returned when the pool closed.
	ErrPoolClosed = errors.New("polite: pool: closed")
)

// Dial returns a function that opens a connection to the database with
// the provided DSN and configuration. The returned function can be assigned
// to the Dial field of a [Pool].
func Dial(dsn string, cfg *Config) func(context.Context) (*Conn, error) {
	return func(ctx context.Context) (*Conn, error) {
		cn, err := Open(ctx, dsn, cfg)
		if err != nil {
			return nil, err
		}
		return cn, nil
	}
}

// TODO(bmizerany): Support multiple hosts. We can keep the DSN on the Conn
// so that on Put, it can pass the DSN to [Pool.Dial], which can take a DSN.
// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-TARGET-SESSION-ATTRS

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
	// If Dial is nil then the package-level Open function is used with
	// [DefaultDSN] and nil config.
	Dial func(ctx context.Context) (*Conn, error)

	// MaxOpen is the maximum number of connections that can be open at
	// once. If MaxOpen is zero, then DefaultPoolMaxConns is used.
	MaxOpen int

	// MaxWaiting is the maximum number of waiters that can be waiting
	// for a connection. If this number is exceeded, Get will return an
	// error. If MaxWaiting is zero, then [DefaultPoolMaxWaiters] is used.
	MaxWaiting int

	// Logger logs pool activity. If Logger is nil, slog.Default() is used.
	Logger *slog.Logger

	waitlist  wait.List[*Conn]
	loadFunc  func() *Conn // load function created once in init
	initOnce  sync.Once
	cancelCtx context.Context
	cancel    context.CancelCauseFunc
}

func (p *Pool) init() {
	p.initOnce.Do(func() {
		p.cancelCtx, p.cancel = context.WithCancelCause(context.Background())
		p.waitlist.MaxItems = p.maxOpen()
		p.waitlist.MaxWaiters = p.maxWaiting()
		p.loadFunc = p.openConn // create method value once during init
	})
}

// Put returns cn to the pool. If the pool is closed, Put closes cn instead.
//
// Put does not block.
func (p *Pool) Put(cn *Conn) {
	p.init()
	if p.cancelCtx.Err() != nil {
		cn.Close()
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
		return nil, cmp.Or(context.Cause(p.cancelCtx), ErrPoolClosed)
	}

	cn, err := p.waitlist.Take(ctx, p.loadFunc)
	switch {
	case errors.Is(err, wait.ErrMaxWaiters):
		return nil, ErrPoolFull
	case errors.Is(err, wait.ErrClosed):
		// Pool was closed while waiting
		return nil, cmp.Or(context.Cause(p.cancelCtx), ErrPoolClosed)
	default:
		return cn, err
	}
}

func (p *Pool) Close() error {
	p.init()
	if p.cancelCtx.Err() != nil {
		return p.cancelCtx.Err()
	}
	p.cancel(ErrPoolClosed)

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
	return Open(ctx, DefaultDSN, nil)
}

func (p *Pool) maxOpen() int {
	if p.MaxOpen == 0 {
		return DefaultPoolMaxConns
	}
	return p.MaxOpen
}

func (p *Pool) maxWaiting() int {
	if p.MaxWaiting == 0 {
		return DefaultPoolMaxWaiters
	}
	return p.MaxWaiting
}

func (p *Pool) log() *slog.Logger {
	if p.Logger == nil {
		return slog.Default()
	}
	return p.Logger
}
