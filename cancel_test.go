package polite

import (
	"context"
	"errors"
	"testing"
	"time"

	"blake.io/polite/febe"
)

// TestQueryCancelDeadline asserts that a context with a short timeout
// actually cancels an in-flight query (pg_sleep), the returned error wraps
// context.DeadlineExceeded, and the connection remains usable.
func TestQueryCancelDeadline(t *testing.T) {
	t.Parallel()

	dsn := startPG(t)
	cn, err := Dial(t.Context(), dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = cn.Exec(ctx, "SELECT pg_sleep(10)")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v; want errors.Is(err, context.DeadlineExceeded)", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("query took %v; expected to be cancelled quickly", elapsed)
	}
	// Underlying febe.Error with code 57014 should still be reachable.
	if fe, ok := errors.AsType[*febe.Error](err); !ok || fe.Code != "57014" {
		t.Fatalf("expected wrapped *febe.Error with code 57014; got %v", err)
	}

	// Connection must be reusable.
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

// TestQueryCancelMidFlight asserts that cancelling the context mid-flight
// terminates a running query, returns context.Canceled, and the connection
// remains usable.
func TestQueryCancelMidFlight(t *testing.T) {
	t.Parallel()

	dsn := startPG(t)
	cn, err := Dial(t.Context(), dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()

	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(150*time.Millisecond, cancel)

	start := time.Now()
	err = cn.Exec(ctx, "SELECT pg_sleep(10)")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want errors.Is(err, context.Canceled)", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("query took %v; expected to be cancelled quickly", elapsed)
	}

	var got int64
	for row := range cn.Query(t.Context(), "SELECT 7") {
		v, err := row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		got = v
	}
	if got != 7 {
		t.Fatalf("SELECT 7 returned %d; want 7", got)
	}
	if !cn.Idle() {
		t.Fatal("connection not idle after recovery")
	}
}

// TestQueryCtxAlreadyCancelled asserts that a ctx already cancelled before
// Query is called returns the ctx error immediately, without sending
// anything to the backend, and the connection remains usable.
func TestQueryCtxAlreadyCancelled(t *testing.T) {
	t.Parallel()

	dsn := startPG(t)
	cn, err := Dial(t.Context(), dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = cn.Exec(ctx, "SELECT 1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}

	var got int64
	for row := range cn.Query(t.Context(), "SELECT 1") {
		v, err := row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		got = v
	}
	if got != 1 {
		t.Fatalf("SELECT 1 returned %d; want 1", got)
	}
}
