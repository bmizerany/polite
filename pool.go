package pqlite

import (
	"cmp"
	"context"
	"errors"
	"iter"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

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

// Errors
var (
	ErrPoolFull = errors.New("pqlite: pool: too many waiting")

	// ErrPoolClosed is returned when the pool closed.
	ErrPoolClosed = errors.New("pqlite: pool: closed")
)

// Opener returns a function that opens a connection to the database with
// the provided DSN and configuration. The returned function can be assigned
// to the Next field of a Pool.
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
// so that on Put, it can pass the DSN to Next, which can take a DSN.
// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-TARGET-SESSION-ATTRS

// Pool is a pool of connections. It is safe for concurrent use by multiple
// goroutines. The zero value is a pool with no limit on the number of
// waiters, ready for use.
type Pool struct {
	// Next is a function that is called to open a new connection when
	// the pool is empty.
	//
	// If Next is nil then DefaultOpener is used.
	Next func(ctx context.Context) (*Conn, error)

	// MaxConns is the maximum number of connections that can be open at
	// once. If MaxConns is zero, then DefaultPoolMaxConns is used.
	MaxOpen int

	// MaxUses is the maximum number of times a connection can be used
	// before it is closed. If MaxUses is zero, then DefaultPoolMaxUses is
	// used.
	MaxUses int

	// MaxWaiting is the maximum number of waiters that can be waiting
	// for a connection. If this number is exceeded, Get will return an
	// error. If MaxWaiting is zero, then DefaultPoolMaxWaiters is used.
	MaxWaiting int

	Logger *slog.Logger // default is slog.Default()

	// TODO(bmizerany): MaxLifetime time.Duration

	readyMu   sync.Mutex // guards the following fields
	ready     queues.Lifo[*Conn]
	openConns int
	cancelCtx context.Context
	cancel    context.CancelCauseFunc

	waitingMu sync.Mutex
	waiting   queues.Fifo[chan *Conn]
}

// makeNewConn is a sentinel value for waiters in the waiting queue to
// indicate that they should stop waiting and make a new connection.
var makeNewConn = new(Conn)

// caller must hold p.readyMu and p.waitingMu
func (p *Pool) putLocked(cn *Conn) {
	handoff := func(cn *Conn) bool {
		ch, ok := p.waiting.Shift()
		if ok {
			// Hand the connection to the first waiter in line. We
			// gurantee nothing is added to the waiting queue that does
			// not immediately start waiting.
			ch <- cn
			return true
		}
		return false
	}

	cn.uses++
	if p.maxUses() > 0 && cn.uses > p.maxUses() {
		defer cn.Close()
		handoff(makeNewConn)
		return
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

// Put adds a connection back to the pool. Put should be called when
// finished with a connection to return it to the pool. If the Conn is
// closed, nothing happens. If the pool is closed, the connection is also
// closed.
//
// Put conn is a non-blocking operation.
func (p *Pool) Put(cn *Conn) {
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

var chanPool = sync.Pool{
	New: func() interface{} {
		return make(chan *Conn, 1)
	},
}

func (p *Pool) startNewConn() {
	for err := range backoff(p.cancelCtx, 5*time.Second) {
		if err != nil {
			p.readyMu.Lock()
			p.openConns--
			p.readyMu.Unlock()
		}
		cn, err := p.next(p.cancelCtx)
		if err == nil {
			p.Put(cn)
			return
		}
		if p.cancelCtx.Err() != nil {
			// This mostly is to prevent logs from being written
			// after tests are done, which is an error.
			return
		}
		p.log().Debug("pqlite: pool: error opening new connection; retrying", "err",
			cmp.Or(context.Cause(p.cancelCtx), err))
	}
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	if ctx.Err() != nil {
		// avoid tiying up resources and delaying a connection from
		// reaching a truly ready waiter if the context is already
		// done.
		return nil, ctx.Err()
	}
	cn, ch, err := func() (*Conn, chan *Conn, error) {
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
			return nil, nil, ErrPoolFull
		}

		// Get in line and wait for another goroutine to put a
		// connection in the pool or for any new connection being
		// opened to be handed off to us.
		ch := chanPool.Get().(chan *Conn)
		p.waiting.Unshift(ch)
		p.waitingMu.Unlock()

		if p.openConns < p.maxOpen() || p.maxOpen() < 0 {
			p.openConns++
			go p.startNewConn()
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
		if ok {
			// Only put the channel back in the pool if it is empty.
			// If we do this for the case of context cancelation, we
			// then a call to Put may pull our chann
			chanPool.Put(ch)
		}
		if cn == nil {
			return nil, ErrPoolClosed
		}
		if cn == makeNewConn {
			return p.next(ctx)
		}
		return cn, nil
	case <-ctx.Done():
		// TODO: remove the channel from the waiting queue
		// TODO: take any connection sent to the channel and put it back in the pool
		p.log().Error("pqlite: pool: TODO: handle context cancelation")
		return nil, ctx.Err()
	}
}

func (p *Pool) Close() error {
	p.readyMu.Lock()
	defer p.readyMu.Unlock()

	if p.cancelCtx == nil {
		p.cancelCtx, p.cancel = context.WithCancelCause(context.Background())
	}

	if p.cancelCtx.Err() != nil {
		return p.cancelCtx.Err()
	}

	p.cancel(ErrPoolClosed)

	// Next, close all ready connections followed by all waiting
	// connections.
	p.waitingMu.Lock()
	defer p.waitingMu.Unlock()

	// Start with everyone waiting so they are not blocked by the ready
	// connections being closed, since closing a channel is (most
	// likely) faster than closing a connection since the conection play
	// nice with postgres and send termination packets which can take
	// time.
	for {
		ch, ok := p.waiting.Shift()
		if !ok {
			break
		}
		close(ch)
	}

	for {
		cn, ok := p.ready.Pop()
		if !ok {
			break
		}
		cn.Close()
	}

	// TODO(bmizerany): wait for all connections to be returned? We
	// should consider taking a context if so, we don't want to block
	// foever if someone is holding a connection or accidentally forgets
	// to return theirs back to the pool.

	return nil
}

func (p *Pool) next(ctx context.Context) (*Conn, error) {
	if p.Next == nil {
		return DefaultOpener(ctx)
	}
	return p.Next(ctx)
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

func backoff(ctx context.Context, maxBackoff time.Duration) iter.Seq[error] {
	var n int
	return func(yield func(error) bool) {
		for {
			if ctx.Err() != nil {
				yield(ctx.Err())
				return
			}

			n++

			// n^2 backoff timer is a little smoother than the
			// common choice of 2^n.
			d := time.Duration(n*n) * 10 * time.Millisecond
			if d > maxBackoff {
				d = maxBackoff
			}
			// Randomize the delay between 0.5-1.5 x msec, in order
			// to prevent accidental "thundering herd" problems.
			d = time.Duration(float64(d) * (rand.Float64() + 0.5))
			t := time.NewTimer(d)
			select {
			case <-ctx.Done():
				t.Stop()
			case <-t.C:
				if !yield(nil) {
					return
				}
			}
		}
	}
}
