// Package list is a tiny, polite-only data layer for the to-do demo.
//
// A [Manager] wraps a [*polite.Pool] with three high-level methods
// (Add, Toggle, Items) and an in-process broadcast bus that fans
// PostgreSQL LISTEN/NOTIFY traffic out to SSE subscribers. Call
// [Manager.Listen] once at startup to bring up the dedicated
// notification connection. The schema's items_notify trigger
// publishes every committed change, so the Go methods are plain
// INSERT and UPDATE statements with no inline pg_notify call.
package list

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"iter"
	"sync"

	"blake.io/polite"
	"blake.io/polite/febe"
)

const channel = "todo_changes"

// ErrInvalidListID is returned by [Manager.Add] and [Manager.Toggle]
// when the listID argument fails the items_list_id_format CHECK
// constraint declared on the items table. Callers should match it
// with [errors.Is] rather than peeking at the underlying Postgres
// error.
var ErrInvalidListID = errors.New("list: invalid list_id")

// Item is a row in the items table. Queries return it as a JSON object
// built by PostgreSQL's json_build_object so the wire bytes drop
// straight into [encoding/json/v2.Unmarshal].
type Item struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
	Done bool   `json:"done"`
}

// Event identifies a list whose items have changed. Subscribers
// re-fetch the list with [Manager.Items] and re-render. The payload
// is intentionally tiny so the trigger has no per-mutation work
// beyond a single pg_notify.
type Event struct {
	ListID string `json:"list_id"`
}

// Manager bundles the pool used for data access and the in-process
// broadcast bus used to fan PostgreSQL LISTEN/NOTIFY traffic out to
// SSE subscribers. One Manager is meant to live for the lifetime of
// the process; construct it with [New], call [Manager.Run] in its
// own goroutine, and reuse the value across HTTP handlers.
type Manager struct {
	dsn  string
	pool *polite.Pool

	mu   sync.Mutex
	subs map[chan Event]string // chan -> listID
}

// New returns a Manager that dials dsn for both pooled queries and
// the dedicated LISTEN connection used by [Manager.Listen]. Data
// methods can be called immediately; live updates require [Manager.Listen]
// to have been called. Call [Manager.Close] to release the pool when
// done.
func New(dsn string) *Manager {
	m := &Manager{
		dsn:  dsn,
		subs: map[chan Event]string{},
	}
	m.pool = &polite.Pool{
		Dial: func(ctx context.Context) (*polite.Conn, error) {
			return polite.Dial(ctx, dsn, nil)
		},
	}
	return m
}

// Close releases the pool's connections.
func (m *Manager) Close() error {
	return m.pool.Close()
}

// Items returns the list's items in insertion order. id is bigserial,
// so ORDER BY id matches insertion order without depending on the
// clock.
func (m *Manager) Items(ctx context.Context, listID string) ([]*Item, error) {
	const q = `SELECT json_build_object('id', id, 'body', body, 'done', done) FROM items WHERE list_id = $1 ORDER BY id`
	var out []*Item
	for row := range m.pool.Query(ctx, q, listID) {
		it, err := decodeItem(row)
		if err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, nil
}

// Add inserts a new item. The items_notify trigger fires after commit
// and broadcasts the change to subscribers. Returns [ErrInvalidListID]
// if listID violates the items_list_id_format CHECK.
func (m *Manager) Add(ctx context.Context, listID, body string) error {
	err := m.pool.Exec(ctx,
		`INSERT INTO items (list_id, body) VALUES ($1, $2)`,
		listID, body)
	return wrapListIDErr(err)
}

// Toggle inverts the done column for one item. The items_notify
// trigger broadcasts the change to subscribers. Returns
// [ErrInvalidListID] if listID violates the items_list_id_format
// CHECK.
func (m *Manager) Toggle(ctx context.Context, listID string, itemID int64) error {
	err := m.pool.Exec(ctx,
		`UPDATE items SET done = NOT done WHERE id = $1 AND list_id = $2`,
		itemID, listID)
	return wrapListIDErr(err)
}

// Events yields events for listID until ctx is cancelled or the
// caller breaks out of the range. The subscription is registered when
// iteration starts and removed when iteration ends, so each call gets
// its own private buffer; nothing leaks if the caller breaks early.
//
// Events are buffered (capacity 16). When the buffer is full the
// dispatch loop drops events for this subscriber rather than stalling
// the rest of the bus; a slow consumer therefore loses updates rather
// than blocking peers.
func (m *Manager) Events(ctx context.Context, listID string) iter.Seq[Event] {
	return func(yield func(Event) bool) {
		ch := make(chan Event, 16)
		m.mu.Lock()
		m.subs[ch] = listID
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.subs, ch)
			m.mu.Unlock()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-ch:
				if !yield(ev) {
					return
				}
			}
		}
	}
}

// Listen dials a dedicated connection and issues the demo's LISTEN
// command, then spawns a goroutine that dispatches notifications to
// matching subscribers until ctx is cancelled. The connection is not
// pool-recycled because LISTEN binds to a specific session; returning
// it to the pool would lose the subscription. Errors during the
// synchronous setup (Dial or LISTEN) are returned; once Listen
// returns nil, the dispatch loop runs in the background.
func (m *Manager) Listen(ctx context.Context) error {
	cn, err := polite.Dial(ctx, m.dsn, nil)
	if err != nil {
		return err
	}
	if err := cn.Exec(ctx, `LISTEN `+channel); err != nil {
		cn.Close()
		return err
	}
	go m.dispatchLoop(ctx, cn)
	return nil
}

func (m *Manager) dispatchLoop(ctx context.Context, cn *polite.Conn) {
	defer cn.Close()
	for n := range polite.Notifications(ctx, cn) {
		var ev Event
		if err := json.Unmarshal(n.Append(nil), &ev); err != nil {
			continue
		}
		m.dispatch(ev)
	}
}

func (m *Manager) dispatch(ev Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for ch, listID := range m.subs {
		if listID != ev.ListID {
			continue
		}
		select {
		case ch <- ev:
		default:
			// Subscriber is behind; drop the event rather than
			// stall dispatch. The next NOTIFY on the same list
			// will re-sync them.
		}
	}
}

// decodeItem unmarshals the first column, a json_build_object
// projection of id, body, and done, into a freshly allocated [Item].
func decodeItem(row *polite.Row) (*Item, error) {
	b, err := row.Column(0).Bytes()
	if err != nil {
		return nil, err
	}
	var it Item
	if err := json.Unmarshal(b, &it); err != nil {
		return nil, err
	}
	return &it, nil
}

// wrapListIDErr translates an items_list_id_format CHECK violation
// into [ErrInvalidListID] so callers do not need to recognise polite
// or Postgres error types. Other errors and nil pass through
// unchanged.
func wrapListIDErr(err error) error {
	if isConstraintError(err, "items_list_id_format") {
		return ErrInvalidListID
	}
	return err
}

// isConstraintError reports whether err is a Postgres error whose
// constraint name matches name. polite surfaces backend errors as
// [*febe.Error] with Constraint set on integrity-violation classes
// (SQLSTATE 23xxx).
func isConstraintError(err error, name string) bool {
	var pgErr *febe.Error
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Constraint == name
}
