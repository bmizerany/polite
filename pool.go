package pqlite

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"blake.io/pqlite/backoff"
	"blake.io/pqlite/wait"
)

// Defaults
const (
	DefaultPoolMaxConns    = 100 // Postgres default
	DefaultPoolMaxWaiters  = 1000
	DefaultPoolMaxUses     = 1000
	DefaultPoolMaxLifetime = 5 * time.Minute

	DefaultDSN = "dbname=postgres host=localhost port=5432 sslmode=disable"
)

// Errors
var (
	ErrPoolFull = errors.New("pqlite: pool: too many waiting")

	// ErrPoolClosed is returned when the pool closed.
	ErrPoolClosed = errors.New("pqlite: pool: closed")
)

// Opener returns a function that opens a connection to the database with
// the provided DSN and configuration. The returned function can be assigned
// to the [Pool.Open] field of a Pool.
func Opener(dsn string, cfg *Config) func(context.Context) (*Conn, error) {
	return func(ctx context.Context) (*Conn, error) {
		cn, err := Open(ctx, dsn, cfg)
		if err != nil {
			return nil, err
		}
		return cn, nil
	}
}

// DefaultOpener returns a function that opens a connection to the database
// using the DefaultDSN with a nil config.
func DefaultOpener(context.Context) (*Conn, error) {
	cn, err := Open(context.Background(), DefaultDSN, nil)
	if err != nil {
		return nil, err
	}
	return cn, nil
}

// TODO(bmizerany): Support multiple hosts. We can keep the DSN on the Conn
// so that on Put, it can pass the DSN to [Pool.Open], which can take a DSN.
// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-TARGET-SESSION-ATTRS

// Pool is a pool of connections. It is safe for concurrent use by multiple
// goroutines. The zero value is ready for use and uses
// [DefaultPoolMaxWaiters] as the waiting limit.
type Pool struct {
	// Open is a function that is called to open a new connection when
	// the pool is empty.
	//
	// If Open is nil then DefaultOpener is used.
	Open func(ctx context.Context) (*Conn, error)

	// MaxOpen is the maximum number of connections that can be open at
	// once. If MaxOpen is zero, then DefaultPoolMaxConns is used.
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

	waitlist  wait.List[*Conn]
	loadFunc  func() (*Conn, error) // load function created once in init
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

// Put adds a connection back to the pool. Put should be called when
// finished with a connection to return it to the pool. If the Conn is
// closed, nothing happens. If the pool is closed, the connection is also
// closed.
//
// Put is a non-blocking operation.
func (p *Pool) Put(cn *Conn) {
	p.init()
	if p.cancelCtx.Err() != nil {
		cn.Close()
		return
	}

	expired := p.MaxLifetime > 0 && time.Since(cn.created) >= p.MaxLifetime
	cn.uses++
	if expired || p.maxUses() > 0 && cn.uses >= p.maxUses() {
		cn.Close()
		go func() {
			if repl, err := p.openConn(); err == nil {
				p.waitlist.Put(repl)
			}
		}()
	} else {
		p.waitlist.Put(cn)
	}
}

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

	if errors.Is(err, wait.ErrMaxWaiters) {
		return nil, ErrPoolFull
	}
	if errors.Is(err, wait.ErrClosed) {
		// Pool was closed while waiting
		return nil, cmp.Or(context.Cause(p.cancelCtx), ErrPoolClosed)
	}
	return cn, err
}

// CloseExpiredConns is kept for compatibility and is currently a no-op.
func (p *Pool) CloseExpiredConns() error {
	// With WaitList, expired connections are handled in Put()
	// This method is kept for compatibility but is now a no-op
	return nil
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

func (p *Pool) openConn() (*Conn, error) {
	for i := range backoff.Loop(p.cancelCtx, 5*time.Second) {
		cn, err := p.open(p.cancelCtx)
		if err == nil {
			cn.created = time.Now()
			return cn, nil
		}
		if p.cancelCtx.Err() != nil {
			return nil, p.cancelCtx.Err()
		}
		if i > 3 {
			p.log().Info("pqlite: pool: error opening new connection; retrying",
				"err", cmp.Or(context.Cause(p.cancelCtx), err))
		}
	}
	return nil, errors.New("failed to open connection after retries")
}

func (p *Pool) open(ctx context.Context) (*Conn, error) {
	if p.Open == nil {
		return DefaultOpener(ctx)
	}
	return p.Open(ctx)
}

func (p *Pool) maxOpen() int {
	if p.MaxOpen == 0 {
		return DefaultPoolMaxConns
	}
	return p.MaxOpen
}

func (p *Pool) maxUses() int {
	if p.MaxUses == 0 {
		return DefaultPoolMaxUses
	}
	return p.MaxUses
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
