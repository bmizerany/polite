// Package list is a tiny, polite-only data layer for the to-do demo.
//
// A [Manager] wraps a [*polite.Pool] with a handful of high-level
// methods (Ensure, Add, Toggle, Items, Get) and an in-process
// LISTEN/NOTIFY broadcast bus that fans Postgres notifications out to
// SSE subscribers. The package is deliberately small: SQL lives next
// to the method that runs it, in a `const q = ...` so the query is
// the documentation.
package list

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"sync"

	"blake.io/polite"
)

// channel is the single PostgreSQL LISTEN channel the demo uses. Every
// mutation NOTIFY's here with a JSON payload that unmarshals into an
// [Event]. A single [Manager] consumes the channel and demultiplexes
// by listID, so we keep one dedicated connection for notifications
// regardless of how many browser tabs are connected.
const channel = "todo_changes"

// Item is a row in the `items` table. Queries return it as a JSON
// object built by `json_build_object` so the wire bytes drop straight
// into [encoding/json/v2.Unmarshal].
type Item struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
	Done bool   `json:"done"`
}

// Event is what [Manager] hands to subscribers. The NOTIFY payload is
// a JSON object produced by `json_build_object` and unmarshaled with
// [encoding/json/v2.Unmarshal].
type Event struct {
	ListID string `json:"list_id"`
	TabID  string `json:"tab_id"`
	ItemID int64  `json:"item_id"`
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

// New returns a Manager that dials dsn for both pooled queries and the
// dedicated LISTEN connection used by [Manager.Run]. The returned value
// is ready to use: data methods can be called immediately. Call
// [Manager.Close] to release the pool when done.
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

// Ensure creates the list row if it does not already exist. The
// ON CONFLICT clause makes the call idempotent so the HTTP handler can
// call it on every page load without a SELECT-then-INSERT race.
func (m *Manager) Ensure(ctx context.Context, listID string) error {
	const q = `INSERT INTO lists (id) VALUES ($1) ON CONFLICT DO NOTHING`
	return m.pool.Exec(ctx, q, listID)
}

// itemJSON is the projection used by every Items/Add/Toggle/Get query;
// it builds the JSON object decodeItem unmarshals.
const itemJSON = `json_build_object('id', id, 'body', body, 'done', done)`

// Items returns the list's items in insertion order (id is bigserial,
// so ORDER BY id is the same as ORDER BY created_at without depending
// on the clock).
func (m *Manager) Items(ctx context.Context, listID string) ([]Item, error) {
	const q = `
		SELECT ` + itemJSON + `
		FROM items
		WHERE list_id = $1
		ORDER BY id
	`
	var out []Item
	for row := range m.pool.Query(ctx, q, listID) {
		it, err := decodeItem(row)
		if err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, nil
}

// Add inserts a new item, NOTIFYs subscribers (so other tabs see it
// over SSE), and returns the inserted row. The INSERT and the NOTIFY
// happen in a single SQL statement using a CTE so the notification can
// only fire if the row is actually committed.
func (m *Manager) Add(ctx context.Context, listID, tabID, body string) (Item, error) {
	const q = `
		WITH inserted AS (
			INSERT INTO items (list_id, body)
			VALUES ($1, $2)
			RETURNING id, body, done
		)
		SELECT ` + itemJSON + `,
		       pg_notify($3, json_build_object('list_id', $1, 'tab_id', $4, 'item_id', id)::text)
		FROM inserted
	`
	for row := range m.pool.Query(ctx, q, listID, body, channel, tabID) {
		return decodeItem(row)
	}
	return Item{}, errors.New("list: insert returned no row")
}

// Toggle flips the `done` flag for a single item. It NOTIFYs with the
// same payload shape as Add; the SSE handler does not distinguish
// inserts from updates — it just re-renders the item by id, and htmx
// out-of-band swaps replace the existing <li> in the DOM.
func (m *Manager) Toggle(ctx context.Context, listID, tabID string, itemID int64) (Item, error) {
	const q = `
		WITH updated AS (
			UPDATE items
			SET done = NOT done
			WHERE id = $1 AND list_id = $2
			RETURNING id, body, done
		)
		SELECT ` + itemJSON + `,
		       pg_notify($3, json_build_object('list_id', $2, 'tab_id', $4, 'item_id', id)::text)
		FROM updated
	`
	for row := range m.pool.Query(ctx, q, itemID, listID, channel, tabID) {
		return decodeItem(row)
	}
	return Item{}, fmt.Errorf("list: item %d not found in %q", itemID, listID)
}

// Get fetches a single item by id. SSE handlers use it to re-render
// the row when a NOTIFY arrives.
func (m *Manager) Get(ctx context.Context, listID string, itemID int64) (Item, error) {
	const q = `SELECT ` + itemJSON + ` FROM items WHERE id = $1 AND list_id = $2`
	for row := range m.pool.Query(ctx, q, itemID, listID) {
		return decodeItem(row)
	}
	return Item{}, fmt.Errorf("list: item %d not found", itemID)
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

// Run dials a dedicated connection and LISTENs on the demo's NOTIFY
// channel for the lifetime of ctx, dispatching every notification to
// matching subscribers. The connection is not pool-recycled because
// LISTEN binds to a specific session; returning it to the pool would
// lose the subscription.
func (m *Manager) Run(ctx context.Context) error {
	cn, err := polite.Dial(ctx, m.dsn, nil)
	if err != nil {
		return err
	}
	defer cn.Close()

	if err := cn.Exec(ctx, `LISTEN `+channel); err != nil {
		return err
	}

	for n := range polite.Notifications(ctx, cn) {
		var ev Event
		if err := json.Unmarshal(n.Payload, &ev); err != nil {
			continue
		}
		m.dispatch(ev)
	}
	return ctx.Err()
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

// decodeItem unmarshals the first column — a `json_build_object`
// projection of (id, body, done) — into an [Item].
func decodeItem(row *polite.Row) (Item, error) {
	b, err := row.Column(0).Bytes()
	if err != nil {
		return Item{}, err
	}
	var it Item
	err = json.Unmarshal(b, &it)
	return it, err
}
