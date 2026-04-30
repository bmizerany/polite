// Command todo is a single-binary to-do list demo for the polite
// PostgreSQL client. It serves a monochrome htmx4 UI with live
// cross-tab updates over Server-Sent Events; every byte of CSS is
// inlined in the template, every byte of JavaScript is htmx itself.
//
// Run it against a local PostgreSQL on port 5432:
//
//	go run ./cmd/todo
//
// Pass -dsn to override the connection string, and -addr to change
// the listen address. Visit http://localhost:8080/ to be redirected
// to a fresh list.
package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"blake.io/polite"
	"blake.io/polite/cmd/todo/list"
)

//go:embed schema.sql
var schemaSQL string

//go:embed templates/index.tmpl
var templateFS embed.FS

//go:embed public
var publicFS embed.FS

// pageData is the value passed to the page and partial templates.
// Items is nil for the OOB partials; the per-item templates project
// the surrounding ListID/TabID into each row via {{$.ListID}} so a
// single Item value is enough.
type pageData struct {
	ListID string
	TabID  string
	Items  []list.Item
}

// itemData is what we hand to the per-item partials. We carry ListID
// and TabID alongside the item itself so the rendered <li> can wire
// up its own toggle button.
type itemData struct {
	ListID string
	TabID  string
	list.Item
}

func main() {
	var (
		dsn  = flag.String("dsn", "host=localhost port=5432 dbname=postgres sslmode=disable", "PostgreSQL DSN (libpq or URL)")
		addr = flag.String("addr", ":8080", "HTTP listen address")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := &polite.Pool{
		Dial: func(ctx context.Context) (*polite.Conn, error) {
			return polite.Dial(ctx, *dsn, nil)
		},
	}
	defer pool.Close()

	if err := applySchema(ctx, pool); err != nil {
		log.Fatalf("schema: %v", err)
	}

	hub := list.NewHub(pool)
	go func() {
		// The Hub owns one connection for LISTEN; if it ever
		// returns we just log and let the demo continue without
		// live updates rather than crashing the server.
		if err := hub.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("hub: %v", err)
		}
	}()

	tmpl := template.Must(template.ParseFS(templateFS, "templates/*.tmpl"))

	srv := &server{pool: pool, hub: hub, tmpl: tmpl}

	mux := http.NewServeMux()
	mux.Handle("GET /public/", http.StripPrefix("/public/", http.FileServer(http.FS(mustSub(publicFS, "public")))))
	mux.HandleFunc("GET /{$}", srv.home)
	mux.HandleFunc("GET /lists/{list}", srv.show)
	mux.HandleFunc("POST /lists/{list}/items", srv.add)
	mux.HandleFunc("POST /lists/{list}/items/{id}/toggle", srv.toggle)
	mux.HandleFunc("GET /lists/{list}/events", srv.events)

	httpSrv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdown)
	}()
	log.Printf("todo listening on %s", *addr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http: %v", err)
	}
}

func applySchema(ctx context.Context, pool *polite.Pool) error {
	cn, err := pool.Get(ctx)
	if err != nil {
		return err
	}
	defer pool.Put(cn)
	// polite prepares every statement, and the extended-query
	// protocol allows only one command per Parse. Split the file
	// on top-level semicolons and run each piece. The schema is
	// plain DDL, so a naive split is safe — no quoted semicolons,
	// no $$-quoted bodies.
	// Strip comments before splitting on ';' — a stray semicolon
	// inside a comment would otherwise cut a statement in half.
	for stmt := range strings.SplitSeq(stripComments(schemaSQL), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := cn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("schema %q: %w", firstLine(stmt), err)
		}
	}
	return nil
}

func stripComments(s string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(s, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

func firstLine(s string) string {
	first, _, _ := strings.Cut(s, "\n")
	return first
}

func mustSub(f embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

type server struct {
	pool *polite.Pool
	hub  *list.Hub
	tmpl *template.Template
}

// home redirects to a fresh list. Lists are identified by a short
// random hex string; this means a user can bookmark a URL to keep
// using the same list, and there is no signup or auth in the demo.
func (s *server) home(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/lists/"+randID(4), http.StatusSeeOther)
}

func (s *server) show(w http.ResponseWriter, r *http.Request) {
	listID := r.PathValue("list")
	if !validID(listID) {
		http.Error(w, "bad list id", http.StatusBadRequest)
		return
	}
	if err := list.Ensure(r.Context(), s.pool, listID); err != nil {
		httpErr(w, err)
		return
	}
	items, err := list.Items(r.Context(), s.pool, listID)
	if err != nil {
		httpErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	render(w, s.tmpl, "page", pageData{
		ListID: listID,
		TabID:  randID(3),
		Items:  items,
	})
}

func (s *server) add(w http.ResponseWriter, r *http.Request) {
	listID := r.PathValue("list")
	if !validID(listID) {
		http.Error(w, "bad list id", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body := strings.TrimSpace(r.PostFormValue("body"))
	tabID := r.PostFormValue("tab")
	if body == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	it, err := list.Add(r.Context(), s.pool, listID, tabID, body)
	if err != nil {
		httpErr(w, err)
		return
	}
	// Append the new row to the originating tab via OOB, and clear
	// the input the same way. Peer tabs receive their copy via the
	// SSE stream below.
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	render(w, s.tmpl, "item-oob-append", itemData{ListID: listID, TabID: tabID, Item: it})
	render(w, s.tmpl, "input-oob", nil)
}

func (s *server) toggle(w http.ResponseWriter, r *http.Request) {
	listID := r.PathValue("list")
	if !validID(listID) {
		http.Error(w, "bad list id", http.StatusBadRequest)
		return
	}
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "bad item id", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tabID := r.PostFormValue("tab")
	it, err := list.Toggle(r.Context(), s.pool, listID, tabID, id)
	if err != nil {
		httpErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	render(w, s.tmpl, "item-oob-replace", itemData{ListID: listID, TabID: tabID, Item: it})
}

// events streams item changes for a list as a single SSE event named
// "oob". The event data is the rendered <li> with hx-swap-oob set, so
// htmx applies the swap in place — there is no client-side dispatch.
//
// The originating tab is identified by the `tab` query parameter and
// filtered out: it has already applied the change via the POST
// response, and re-applying the OOB swap would either duplicate the
// row (for inserts) or briefly flicker (for toggles).
func (s *server) events(w http.ResponseWriter, r *http.Request) {
	listID := r.PathValue("list")
	if !validID(listID) {
		http.Error(w, "bad list id", http.StatusBadRequest)
		return
	}
	tabID := r.URL.Query().Get("tab")
	log.Printf("sse open  list=%s tab=%s remote=%s", listID, tabID, r.RemoteAddr)
	defer log.Printf("sse close list=%s tab=%s", listID, tabID)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	sub := s.hub.Subscribe(listID)
	defer s.hub.Unsubscribe(sub)

	// Track which item IDs this connection has already shown so we
	// can choose between "append" (new) and "replace" (update). The
	// initial set comes from the items we know are already in the
	// page; we lazily fill it as events arrive.
	seen, err := initialSeen(r.Context(), s.pool, listID)
	if err != nil {
		httpErr(w, err)
		return
	}

	// Heartbeat keeps proxies and load balancers from closing the
	// idle connection. SSE comments (": ...\n\n") are ignored by
	// the client.
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()

	enc := sseWriter{w: w, flusher: flusher}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			enc.comment("ping")
		case ev, ok := <-sub:
			if !ok {
				return
			}
			// Mark seen *before* the originator filter: when this
			// tab's own POST inserts a row via OOB, that row is
			// now present in the tab's DOM even though we never
			// emit an SSE message for it. Without this, a later
			// toggle on the same row would arrive marked as
			// "append" and create a duplicate <li>.
			isFirst := !seen[ev.ItemID]
			seen[ev.ItemID] = true
			if ev.TabID == tabID {
				continue
			}
			it, err := list.Get(r.Context(), s.pool, ev.ListID, ev.ItemID)
			if err != nil {
				continue
			}
			tmplName := "item-oob-replace"
			if isFirst {
				tmplName = "item-oob-append"
			}

			var b strings.Builder
			if err := s.tmpl.ExecuteTemplate(&b, tmplName,
				itemData{ListID: ev.ListID, TabID: tabID, Item: it}); err != nil {
				log.Printf("render: %v", err)
				continue
			}
			enc.message(b.String())
		}
	}
}

func initialSeen(ctx context.Context, pool *polite.Pool, listID string) (map[int64]bool, error) {
	items, err := list.Items(ctx, pool, listID)
	if err != nil {
		return nil, err
	}
	out := make(map[int64]bool, len(items))
	for _, it := range items {
		out[it.ID] = true
	}
	return out, nil
}

// sseWriter serializes Server-Sent Events. We keep it inline rather
// than pulling in a third-party package; the wire format is just
// "event: name\ndata: line\n\n".
type sseWriter struct {
	mu      sync.Mutex
	w       http.ResponseWriter
	flusher http.Flusher
}

// message writes an unnamed SSE message. htmx 4's hx-sse extension
// auto-swaps unnamed messages via htmx.swap, which honors any
// hx-swap-oob attributes inside the data — exactly what we want.
func (s *sseWriter) message(data string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for line := range strings.SplitSeq(data, "\n") {
		fmt.Fprintf(s.w, "data: %s\n", line)
	}
	fmt.Fprint(s.w, "\n")
	s.flusher.Flush()
}

func (s *sseWriter) comment(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.w, ": %s\n\n", msg)
	s.flusher.Flush()
}

// validID limits list IDs to a small charset so they are safe to drop
// straight into the page title and into URLs without escaping. The
// HTTP handlers still pass them as parameters to polite, so SQL
// injection is not the concern here — readability is.
func validID(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}

func randID(nBytes int) string {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func render(w http.ResponseWriter, t *template.Template, name string, data any) {
	if err := t.ExecuteTemplate(w, name, data); err != nil {
		log.Printf("template %s: %v", name, err)
	}
}

func httpErr(w http.ResponseWriter, err error) {
	log.Printf("error: %v", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
