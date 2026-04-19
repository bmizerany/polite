package polite

import (
	"context"
	"errors"
	"testing"
	"time"

	"blake.io/polite/febe"
)

func TestQueryDeadlineAllowsReuse(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	var gotErr error
	for row := range cn.Query(ctx, "SELECT pg_sleep(1)") {
		if _, err := row.Column(0).Bytes(); err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatal("expected deadline error")
	}
	if !errors.Is(gotErr, context.DeadlineExceeded) {
		t.Fatalf("Query() error = %v; want %v", gotErr, context.DeadlineExceeded)
	}

	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) after timeout = %v; want nil", err)
	}
}

// TestQueryCancelMidflight starts a long-sleeping query, cancels its
// context from another goroutine, and asserts the first row's error wraps
// context.Canceled. The connection must remain reusable afterwards.
func TestQueryCancelMidflight(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	var gotErr error
	for row := range cn.Query(ctx, "SELECT pg_sleep(3)") {
		if _, err := row.Column(0).Bytes(); err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatal("expected cancel error")
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("Query() error = %v; want %v", gotErr, context.Canceled)
	}

	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) after cancel = %v; want nil", err)
	}
}

// TestExecDeadlineAllowsReuse mirrors TestQueryDeadlineAllowsReuse but for
// Exec: a 100ms deadline against pg_sleep(1) must return an error wrapping
// context.DeadlineExceeded, after which the conn is still usable.
func TestExecDeadlineAllowsReuse(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := cn.Exec(ctx, "SELECT pg_sleep(1)")
	if err == nil {
		t.Fatal("expected deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec() error = %v; want %v", err, context.DeadlineExceeded)
	}

	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) after timeout = %v; want nil", err)
	}
}

// TestQueryPrecanceledContext cancels the ctx before calling Query. The
// first row's column error must wrap context.Canceled and the conn must
// remain idle and reusable.
func TestQueryPrecanceledContext(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var gotErr error
	for row := range cn.Query(ctx, "SELECT 1") {
		if _, err := row.Column(0).Bytes(); err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatal("expected cancel error")
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("Query() error = %v; want %v", gotErr, context.Canceled)
	}

	if got := cn.fc.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("Status() = %q; want %q", got, febe.ReadyStatusIdle)
	}
	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) after pre-cancel = %v; want nil", err)
	}
}

// TestExecPrecanceledContext cancels the ctx before calling Exec. The
// error must wrap context.Canceled and the conn must remain idle and
// reusable.
func TestExecPrecanceledContext(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := cn.Exec(ctx, "SELECT 1")
	if err == nil {
		t.Fatal("expected cancel error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Exec() error = %v; want %v", err, context.Canceled)
	}

	if got := cn.fc.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("Status() = %q; want %q", got, febe.ReadyStatusIdle)
	}
	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) after pre-cancel = %v; want nil", err)
	}
}

// TestQueryCanceledMidStream reads a single row from a large stream, then
// cancels the ctx and breaks out. The drain path after the yield loop
// must cope with the cancelled ctx and leave the conn reusable.
func TestQueryCanceledMidStream(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	rows := 0
	for row := range cn.Query(ctx, "SELECT * FROM generate_series(1, 10000)") {
		if _, err := row.Column(0).Bytes(); err != nil {
			t.Fatalf("decode row: %v", err)
		}
		rows++
		cancel()
		break
	}
	if rows == 0 {
		t.Fatal("expected at least one row before cancel")
	}

	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) after mid-stream cancel = %v; want nil", err)
	}
}

// TestStatusIdleAfterCanceledQuery runs the deadline-cancellation flow
// and directly checks that the febe conn returned to ReadyForQuery idle,
// which is the concrete promise made by Conn's doc comment.
func TestStatusIdleAfterCanceledQuery(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	var gotErr error
	for row := range cn.Query(ctx, "SELECT pg_sleep(1)") {
		if _, err := row.Column(0).Bytes(); err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatal("expected deadline error")
	}
	if !errors.Is(gotErr, context.DeadlineExceeded) {
		t.Fatalf("Query() error = %v; want %v", gotErr, context.DeadlineExceeded)
	}

	if got := cn.fc.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("Status() = %q; want %q", got, febe.ReadyStatusIdle)
	}
}
