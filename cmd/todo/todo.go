// Command todo is a single-binary to-do list demo for the polite
// PostgreSQL client. It serves a monochrome htmx4 UI with live
// cross-tab updates over Server-Sent Events. The Postgres schema
// owns change notification through an AFTER INSERT OR UPDATE trigger,
// so the Go side runs plain INSERT and UPDATE statements. Every
// committed change broadcasts a tiny LISTEN/NOTIFY message; SSE
// subscribers re-fetch the list and re-render <ul id="items"> in
// place via htmx out-of-band swap.
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
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"blake.io/polite"
	"blake.io/polite/cmd/todo/list"
)

//go:embed templates/index.tmpl
var templateFS embed.FS

//go:embed public
var publicFS embed.FS

// pageData is the value passed to both the page template and the
// items partial. The same value drives the initial page render and
// every SSE-driven re-render of <ul id="items">.
type pageData struct {
	ListID string
	Items  []*list.Item
}

func main() {
	var (
		dsn  = flag.String("dsn", "host=localhost port=5432 dbname=postgres sslmode=disable", "PostgreSQL DSN (libpq or URL)")
		addr = flag.String("addr", ":8080", "HTTP listen address")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := applySchema(ctx, *dsn); err != nil {
		log.Fatalf("schema: %v", err)
	}

	mgr := list.New(*dsn)
	defer mgr.Close()
	if err := mgr.Listen(ctx); err != nil {
		log.Fatalf("listen: %v", err)
	}

	tmpl := template.Must(template.ParseFS(templateFS, "templates/*.tmpl"))

	srv := &server{mgr: mgr, tmpl: tmpl}

	mux := http.NewServeMux()
	mux.Handle("GET /public/", http.StripPrefix("/public/", http.FileServer(http.FS(mustSub(publicFS, "public")))))
	mux.HandleFunc("GET /{$}", srv.handleHome)
	mux.HandleFunc("GET /lists/{list}", srv.handleShow)
	mux.HandleFunc("POST /lists/{list}/items", srv.handleAdd)
	mux.HandleFunc("POST /lists/{list}/items/{id}/toggle", srv.handleToggle)
	mux.HandleFunc("GET /lists/{list}/events", srv.handleEvents)

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

func mustSub(f embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

type server struct {
	mgr  *list.Manager
	tmpl *template.Template
}

// handleHome mints a fresh random-hex list ID and redirects so the
// URL is bookmarkable; the demo has no signup or auth.
func (s *server) handleHome(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/lists/"+randID(4), http.StatusSeeOther)
}

func (s *server) handleShow(w http.ResponseWriter, r *http.Request) {
	listID := r.PathValue("list")
	items, err := s.mgr.Items(r.Context(), listID)
	if err != nil {
		httpErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	render(w, s.tmpl, "page", pageData{ListID: listID, Items: items})
}

func (s *server) handleAdd(w http.ResponseWriter, r *http.Request) {
	listID := r.PathValue("list")
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body := strings.TrimSpace(r.PostFormValue("body"))
	if body == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := s.mgr.Add(r.Context(), listID, body); err != nil {
		if errors.Is(err, list.ErrInvalidListID) {
			http.Error(w, "bad list id", http.StatusBadRequest)
			return
		}
		httpErr(w, err)
		return
	}
	// The new item arrives in this tab the same way it arrives in
	// peer tabs: through the SSE stream, which re-renders the list
	// in place. The POST response only clears the input.
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	render(w, s.tmpl, "input-oob", nil)
}

func (s *server) handleToggle(w http.ResponseWriter, r *http.Request) {
	listID := r.PathValue("list")
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "bad item id", http.StatusBadRequest)
		return
	}
	if err := s.mgr.Toggle(r.Context(), listID, id); err != nil {
		httpErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleEvents streams change notifications for a list. On every
// NOTIFY the handler re-fetches the list and writes the rendered
// <ul> as one SSE message. The <ul> carries hx-swap-oob="outerHTML"
// so htmx applies the swap in place; the originating tab and peer
// tabs are updated identically by the same mechanism.
func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	listID := r.PathValue("list")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	for range s.mgr.Events(r.Context(), listID) {
		items, err := s.mgr.Items(r.Context(), listID)
		if err != nil {
			log.Printf("items: %v", err)
			continue
		}
		var b strings.Builder
		if err := s.tmpl.ExecuteTemplate(&b, "items",
			pageData{ListID: listID, Items: items}); err != nil {
			log.Printf("render: %v", err)
			continue
		}
		writeSSE(w, b.String())
		flusher.Flush()
	}
}

// writeSSE writes one SSE message frame: each line of html is
// prefixed with "data: " per the SSE wire format, and the message
// terminates with a blank line.
func writeSSE(w io.Writer, html string) {
	for line := range strings.SplitSeq(html, "\n") {
		fmt.Fprintf(w, "data: %s\n", line)
	}
	fmt.Fprint(w, "\n")
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

//go:embed schema.sql
var schemaSQL string

// applySchema executes schemaSQL one statement at a time. polite
// prepares every statement and the extended-query protocol allows
// only one command per Parse.
func applySchema(ctx context.Context, dsn string) error {
	cn, err := polite.Dial(ctx, dsn, nil)
	if err != nil {
		return err
	}
	defer cn.Close()
	stmts, err := splitSchema(schemaSQL)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if err := cn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	return nil
}

// splitSchema returns the statements in s. Statements are separated
// by a line whose trimmed content is exactly ";". A line that begins
// with ";" but carries anything else is rejected: keep the separator
// alone so the splitter never has to know about $$-quoting, single
// quotes, or "--" comments.
func splitSchema(s string) ([]string, error) {
	var out []string
	var cur strings.Builder
	for line := range strings.Lines(s) {
		t := strings.TrimSpace(line)
		if t == ";" {
			if stmt := strings.TrimSpace(cur.String()); stmt != "" {
				out = append(out, stmt)
			}
			cur.Reset()
			continue
		}
		if strings.HasPrefix(t, ";") {
			return nil, fmt.Errorf("schema: %q: ';' must be alone on its line", line)
		}
		cur.WriteString(line)
	}
	if stmt := strings.TrimSpace(cur.String()); stmt != "" {
		out = append(out, stmt)
	}
	return out, nil
}
