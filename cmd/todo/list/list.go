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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"blake.io/polite"
)

// Channel is the single PostgreSQL LISTEN channel the demo uses. Every
// mutation NOTIFY's here with a payload of `listID|tabID|itemID`. A
// single [Manager] consumes the channel and demultiplexes by listID, so
// we keep one dedicated connection for notifications regardless of
// how many browser tabs are connected.
const Channel = "todo_changes"

// Item is a row in the `items` table.
type Item struct {
	ID   int64
	Body string
	Done bool
}

// Event is what [Manager] hands to subscribers. ListID and TabID come
// from the NOTIFY payload; ItemID is the id of the row that changed.
type Event struct {
	ListID string
	TabID  string
	ItemID int64
}

// Manager bundles the pool used for data access and the in-process
// broadcast bus used to fan PostgreSQL LISTEN/NOTIFY traffic out to
// SSE subscribers. One Manager is meant to live for the lifetime of
// the process; construct it with [New], call [Manager.Run] in its
// own goroutine, and reuse the value across HTTP handlers.
type Manager struct {
	pool *polite.Pool

	mu   sync.Mutex
	subs map[chan Event]string // chan -> listID
}

// New returns a Manager bound to pool. The returned value is ready to
// use: data methods can be called immediately; [Manager.Run] starts the
// LISTEN/NOTIFY worker.
func New(pool *polite.Pool) *Manager {
	return &Manager{pool: pool, subs: map[chan Event]string{}}
}

// Ensure creates the list row if it does not already exist. The
// ON CONFLICT clause makes the call idempotent so the HTTP handler can
// call it on every page load without a SELECT-then-INSERT race.
func (m *Manager) Ensure(ctx context.Context, listID string) error {
	const q = `INSERT INTO lists (id) VALUES ($1) ON CONFLICT DO NOTHING`
	return m.pool.Exec(ctx, q, listID)
}

// Items returns the list's items in insertion order (id is bigserial,
// so ORDER BY id is the same as ORDER BY created_at without depending
// on the clock).
func (m *Manager) Items(ctx context.Context, listID string) ([]Item, error) {
	const q = `
		SELECT id, body, done
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
		SELECT id, body, done,
		       pg_notify($3, $1 || '|' || $4 || '|' || id::text)
		FROM inserted
	`
	for row := range m.pool.Query(ctx, q, listID, body, Channel, tabID) {
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
		SELECT id, body, done,
		       pg_notify($3, $2 || '|' || $4 || '|' || id::text)
		FROM updated
	`
	for row := range m.pool.Query(ctx, q, itemID, listID, Channel, tabID) {
		return decodeItem(row)
	}
	return Item{}, fmt.Errorf("list: item %d not found in %q", itemID, listID)
}

// Get fetches a single item by id. SSE handlers use it to re-render
// the row when a NOTIFY arrives.
func (m *Manager) Get(ctx context.Context, listID string, itemID int64) (Item, error) {
	const q = `SELECT id, body, done FROM items WHERE id = $1 AND list_id = $2`
	for row := range m.pool.Query(ctx, q, itemID, listID) {
		return decodeItem(row)
	}
	return Item{}, fmt.Errorf("list: item %d not found", itemID)
}

// Subscribe registers a channel to receive events for listID.
// Unsubscribe must be called to release the slot. The returned channel
// is buffered so a slow subscriber cannot stall the dispatch loop;
// events are dropped (rather than blocking) when the buffer fills.
func (m *Manager) Subscribe(listID string) chan Event {
	ch := make(chan Event, 16)
	m.mu.Lock()
	m.subs[ch] = listID
	m.mu.Unlock()
	return ch
}

// Unsubscribe removes ch from the broadcast set and closes it.
func (m *Manager) Unsubscribe(ch chan Event) {
	m.mu.Lock()
	delete(m.subs, ch)
	m.mu.Unlock()
	close(ch)
}

// Run owns one polite connection for the lifetime of ctx. It LISTENs
// on [Channel] and dispatches every notification to matching
// subscribers. Run uses its own connection (not pool-recycled) because
// LISTEN binds to a specific session; returning the connection to the
// pool would lose the subscription.
func (m *Manager) Run(ctx context.Context) error {
	cn, err := m.pool.Get(ctx)
	if err != nil {
		return err
	}
	// We deliberately do not Put this connection back: it is
	// dedicated to Run for its lifetime.
	defer cn.Close()

	if err := cn.Exec(ctx, `LISTEN `+quoteIdent(Channel)); err != nil {
		return err
	}

	for n := range polite.Notifications(ctx, cn) {
		ev, ok := parsePayload(string(n.Payload))
		if !ok {
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

// decodeItem reads (id, body, done) — the three columns every
// query in this package selects in that order — into an Item.
func decodeItem(row *polite.Row) (Item, error) {
	if _, err := row.Column(0).Bytes(); err != nil {
		return Item{}, err
	}
	var it Item
	it.ID, _ = row.Column(0).DecodeInt()
	it.Body, _ = row.Column(1).DecodeText()
	it.Done, _ = row.Column(2).DecodeBool()
	return it, nil
}

func parsePayload(s string) (Event, bool) {
	parts := strings.SplitN(s, "|", 3)
	if len(parts) != 3 {
		return Event{}, false
	}
	id, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return Event{}, false
	}
	return Event{ListID: parts[0], TabID: parts[1], ItemID: id}, true
}

// quoteIdent quotes a SQL identifier the way PostgreSQL expects:
// wrap in double quotes and double any embedded ones. We only ever
// pass [Channel] through here, but keeping the quoting honest lets a
// reader trust the call site at a glance.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
