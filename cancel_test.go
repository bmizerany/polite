package polite

import (
	"context"
	"errors"
	"testing"
	"time"

	"blake.io/polite/febe"
	"blake.io/polite/internal/backoff"
)

// assertReusable asserts cn can run a trivial query and ends idle.
// A cancelled connection that did not properly drain to ReadyForQuery
// will fail one or both checks.
func assertReusable(t *testing.T, cn *Conn) {
	t.Helper()
	var got int64
	for row := range cn.Query(t.Context(), "SELECT 42") {
		v, err := row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		got = v
	}
	if got != 42 {
		t.Fatalf("SELECT 42 returned %d; want 42", got)
	}
	if !cn.Idle() {
		t.Fatal("connection not idle after recovery")
	}
}

// assertQueryCanceled checks the standard contract for a cancelled
// in-flight query: the returned error wraps the given ctx error and
// also wraps a *febe.Error with SQLSTATE 57014 (query_canceled).
func assertQueryCanceled(t *testing.T, err error, want error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, want) {
		t.Fatalf("err = %v; want errors.Is(err, %v)", err, want)
	}
	var fe *febe.Error
	if !errors.As(err, &fe) || fe.Code != "57014" {
		t.Fatalf("expected wrapped *febe.Error with code 57014; got %v", err)
	}
}

// TestQueryCancelDeadline asserts that a context deadline interrupts
// an in-flight pg_sleep, the returned error wraps DeadlineExceeded
// and the backend's 57014 query_canceled, and the connection remains
// usable. The deadline is many orders of magnitude shorter than the
// pg_sleep duration, so a missed cancel is unambiguous.
func TestQueryCancelDeadline(t *testing.T) {
	t.Parallel()
	cn := newSession(t)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := cn.Exec(ctx, "SELECT pg_sleep(31536000)")
	assertQueryCanceled(t, err, context.DeadlineExceeded)
	assertReusable(t, cn)
}

// TestQueryCancelMidFlight asserts that explicitly cancelling ctx
// while a query is running on the backend returns context.Canceled
// wrapping the backend's 57014. Rather than fire cancel from a timer
// (racy in principle, and undermined by PostgreSQL's small-result
// buffering), this test uses a second "monitor" connection to poll
// pg_stat_activity and only fires cancel once PostgreSQL itself
// reports the test backend has flipped to state='active' executing
// the pg_sleep. That is the precise condition under which
// CancelRequest is honored, so the test is deterministic with
// respect to backend state, not wall time.
func TestQueryCancelMidFlight(t *testing.T) {
	t.Parallel()
	cn := newSession(t)
	mon := newSession(t)

	// Capture the test backend's PID so the monitor knows which
	// backend to watch.
	var pid int64
	for row := range cn.Query(t.Context(), "SELECT pg_backend_pid()") {
		v, err := row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		pid = v
	}
	if pid == 0 {
		t.Fatal("failed to capture backend pid")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- cn.Exec(ctx, "SELECT pg_sleep(31536000)")
	}()

	// Poll until the test backend is actively executing the
	// pg_sleep. backoff.Loop yields immediately on the first
	// iteration, so the first poll catches the (common) case where
	// the backend has already activated by the time we get here;
	// subsequent iterations back off polynomially, capped at 50ms,
	// so a slow start does not burn CPU. The pollCtx deadline is
	// generous but bounded so a regression cannot hang CI.
	pollCtx, pollCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer pollCancel()
	for _, err := range backoff.Loop(pollCtx, 50*time.Millisecond) {
		if err != nil {
			t.Fatal("backend never reached state=active within poll deadline")
		}
		rows := 0
		for range mon.Query(pollCtx, `
			SELECT 1 FROM pg_stat_activity
			WHERE pid = $1 AND state = 'active' AND query LIKE '%pg_sleep%'
		`, int32(pid)) {
			rows++
		}
		if rows > 0 {
			break
		}
	}

	cancel()

	err := <-done
	assertQueryCanceled(t, err, context.Canceled)
	assertReusable(t, cn)
}

// TestQueryCtxAlreadyCancelled asserts that a ctx already cancelled
// before Exec returns the ctx error and the connection remains
// usable. There is no backend round trip to cancel.
func TestQueryCtxAlreadyCancelled(t *testing.T) {
	t.Parallel()
	cn := newSession(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := cn.Exec(ctx, "SELECT 1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
	assertReusable(t, cn)
}
