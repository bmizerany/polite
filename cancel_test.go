package polite

import (
	"context"
	"errors"
	"testing"
	"time"

	"blake.io/polite/febe"
)

// dialForCancelTest opens a fresh Conn and registers cleanup. Tests
// share one PG via startPG but each owns its own connection so a
// cancellation on one cannot affect another.
func dialForCancelTest(t *testing.T) *Conn {
	t.Helper()
	dsn := startPG(t)
	cn, err := Dial(t.Context(), dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cn.Close() })
	return cn
}

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
// usable. The deadline is short enough that exceeding it by orders of
// magnitude (i.e. waiting on the full 10-second sleep) is unambiguous.
func TestQueryCancelDeadline(t *testing.T) {
	t.Parallel()
	cn := dialForCancelTest(t)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := cn.Exec(ctx, "SELECT pg_sleep(10)")
	assertQueryCanceled(t, err, context.DeadlineExceeded)
	assertReusable(t, cn)
}

// TestQueryCancelMidFlight asserts that explicitly cancelling ctx
// while pg_sleep is running returns context.Canceled wrapping the
// backend's 57014. The 200ms delay is comfortably longer than the
// time it takes the query to enter pg_sleep on the backend, and is
// orders of magnitude shorter than the 10s sleep duration, so the
// elapsed-time bound below is unambiguous if cancellation breaks.
//
// TODO(bmizerany): replace the timer with a febe.Trace driven cancel
// once the watcher reliably delivers CancelRequest when fired within
// the first millisecond of Execute.
func TestQueryCancelMidFlight(t *testing.T) {
	t.Parallel()
	cn := dialForCancelTest(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	time.AfterFunc(200*time.Millisecond, cancel)

	start := time.Now()
	err := cn.Exec(ctx, "SELECT pg_sleep(10)")
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("query took %v; cancellation did not interrupt pg_sleep", d)
	}
	assertQueryCanceled(t, err, context.Canceled)
	assertReusable(t, cn)
}

// TestQueryCtxAlreadyCancelled asserts that a ctx already cancelled
// before Exec returns the ctx error and the connection remains
// usable. There is no backend round trip to cancel.
func TestQueryCtxAlreadyCancelled(t *testing.T) {
	t.Parallel()
	cn := dialForCancelTest(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := cn.Exec(ctx, "SELECT 1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
	assertReusable(t, cn)
}
