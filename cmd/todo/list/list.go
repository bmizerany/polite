// Package list is a tiny, polite-only data layer for the to-do demo.
//
// It wraps a [*polite.Pool] with a handful of high-level functions
// (Ensure, Add, Toggle, Items) and a [Hub] that fans PostgreSQL
// LISTEN/NOTIFY traffic out to in-process subscribers. The package is
// deliberately small: SQL lives next to the function that runs it, in
// a `const q = ...` so the query is the documentation.
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
// single [Hub] consumes the channel and demultiplexes by listID, so we
// keep one dedicated connection for notifications regardless of how
// many browser tabs are connected.
const Channel = "todo_changes"

// Item is a row in the `items` table.
type Item struct {
	ID   int64
	Body string
	Done bool
}

// Ensure creates the list row if it does not already exist. The
// ON CONFLICT clause makes the call idempotent so the HTTP handler can
// call it on every page load without a SELECT-then-INSERT race.
func Ensure(ctx context.Context, pool *polite.Pool, listID string) error {
	const q = `INSERT INTO lists (id) VALUES ($1) ON CONFLICT DO NOTHING`
	return pool.Exec(ctx, q, listID)
}

// Items returns the list's items in insertion order (id is bigserial,
// so ORDER BY id is the same as ORDER BY created_at without depending
// on the clock).
func Items(ctx context.Context, pool *polite.Pool, listID string) ([]Item, error) {
	const q = `
		SELECT id, body, done
		FROM items
		WHERE list_id = $1
		ORDER BY id
	`
	var out []Item
	for row := range pool.Query(ctx, q, listID) {
		if _, err := row.Column(0).Bytes(); err != nil {
			return nil, err
		}
		var it Item
		it.ID, _ = row.Column(0).DecodeInt()
		it.Body, _ = row.Column(1).DecodeText()
		it.Done = decodeBool(row)
		out = append(out, it)
	}
	return out, nil
}

// decodeBool reads a boolean from the third column. Polite does not
// have a built-in bool decoder (it rejects bool arguments on the
// encoding side too), so we read the single wire byte directly: bool's
// binary format is one byte, 0x00 or 0x01.
func decodeBool(row *polite.Row) bool {
	b, err := row.Column(2).Bytes()
	if err != nil || len(b) == 0 {
		return false
	}
	return b[0] != 0
}

// Add inserts a new item, NOTIFYs subscribers (so other tabs see it
// over SSE), and returns the inserted row. The INSERT and the NOTIFY
// happen in a single SQL statement using a CTE so the notification can
// only fire if the row is actually committed.
func Add(ctx context.Context, pool *polite.Pool, listID, tabID, body string) (Item, error) {
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
	var it Item
	for row := range pool.Query(ctx, q, listID, body, Channel, tabID) {
		if _, err := row.Column(0).Bytes(); err != nil {
			return Item{}, err
		}
		it.ID, _ = row.Column(0).DecodeInt()
		it.Body, _ = row.Column(1).DecodeText()
		it.Done = decodeBool(row)
	}
	if it.ID == 0 {
		return Item{}, errors.New("list: insert returned no row")
	}
	return it, nil
}

// Toggle flips the `done` flag for a single item. It NOTIFYs with the
// same payload shape as Add; the SSE handler does not distinguish
// inserts from updates — it just re-renders the item by id, and htmx
// out-of-band swaps replace the existing <li> in the DOM.
func Toggle(ctx context.Context, pool *polite.Pool, listID, tabID string, itemID int64) (Item, error) {
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
	var it Item
	for row := range pool.Query(ctx, q, itemID, listID, Channel, tabID) {
		if _, err := row.Column(0).Bytes(); err != nil {
			return Item{}, err
		}
		it.ID, _ = row.Column(0).DecodeInt()
		it.Body, _ = row.Column(1).DecodeText()
		it.Done = decodeBool(row)
	}
	if it.ID == 0 {
		return Item{}, fmt.Errorf("list: item %d not found in %q", itemID, listID)
	}
	return it, nil
}

// Get fetches a single item by id. SSE handlers use it to re-render
// the row when a NOTIFY arrives.
func Get(ctx context.Context, pool *polite.Pool, listID string, itemID int64) (Item, error) {
	const q = `SELECT id, body, done FROM items WHERE id = $1 AND list_id = $2`
	var it Item
	var found bool
	for row := range pool.Query(ctx, q, itemID, listID) {
		if _, err := row.Column(0).Bytes(); err != nil {
			return Item{}, err
		}
		it.ID, _ = row.Column(0).DecodeInt()
		it.Body, _ = row.Column(1).DecodeText()
		it.Done = decodeBool(row)
		found = true
	}
	if !found {
		return Item{}, fmt.Errorf("list: item %d not found", itemID)
	}
	return it, nil
}

// Event is what the [Hub] hands to subscribers. ListID and TabID come
// from the NOTIFY payload; ItemID is the id of the row that changed.
type Event struct {
	ListID string
	TabID  string
	ItemID int64
}

// Hub fans PostgreSQL LISTEN/NOTIFY traffic out to in-process
// subscribers. One Hub holds one dedicated polite connection; every
// SSE handler subscribes through the Hub instead of opening its own
// LISTEN connection, which would pin one DB connection per browser
// tab.
type Hub struct {
	pool *polite.Pool

	mu   sync.Mutex
	subs map[chan Event]string // chan -> listID
}

// NewHub returns a Hub bound to pool. Run must be called exactly once
// in its own goroutine; it returns when ctx is cancelled or the
// underlying connection fails terminally.
func NewHub(pool *polite.Pool) *Hub {
	return &Hub{pool: pool, subs: map[chan Event]string{}}
}

// Subscribe registers a channel to receive events for listID.
// Unsubscribe must be called to release the slot. The returned channel
// is buffered so a slow subscriber cannot stall the Hub's read loop;
// events are dropped (rather than blocking) when the buffer fills.
func (h *Hub) Subscribe(listID string) chan Event {
	ch := make(chan Event, 16)
	h.mu.Lock()
	h.subs[ch] = listID
	h.mu.Unlock()
	return ch
}

// Unsubscribe removes ch from the Hub and closes it.
func (h *Hub) Unsubscribe(ch chan Event) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
	close(ch)
}

// Run owns one polite connection for the lifetime of ctx. It LISTENs
// on [Channel] and dispatches every notification to matching
// subscribers. Run uses its own connection (not the pool) because
// LISTEN binds to a specific session; recycling the connection back
// into the pool would lose the subscription.
func (h *Hub) Run(ctx context.Context) error {
	cn, err := h.pool.Get(ctx)
	if err != nil {
		return err
	}
	// We deliberately do not Put this connection back: it is
	// dedicated to the Hub for the duration of Run.
	defer cn.Close()

	if err := cn.Exec(ctx, `LISTEN `+quoteIdent(Channel)); err != nil {
		return err
	}

	for n := range polite.Notifications(ctx, cn) {
		ev, ok := parsePayload(string(n.Payload))
		if !ok {
			continue
		}
		h.dispatch(ev)
	}
	return ctx.Err()
}

func (h *Hub) dispatch(ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch, listID := range h.subs {
		if listID != ev.ListID {
			continue
		}
		select {
		case ch <- ev:
		default:
			// Subscriber is behind; drop the event rather than
			// stall the Hub. The next NOTIFY on the same list
			// will re-sync them.
		}
	}
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
