package polite

import (
	"context"
	"errors"
	"testing"
	"time"

	"blake.io/polite/febe"
)

// TestNotificationsCancelReturnsAfterNextMessage documents the post-hoc
// cancellation behavior of Notifications: because the ctx check happens at
// the top of febe.Conn.ReadHeader's loop, a cancel() alone cannot unblock a
// Read that is waiting for the next server message. The iterator returns
// only once another message arrives (here, a NOTIFY from a second conn)
// and the loop iterates to observe ctx.Err().
func TestNotificationsCancelReturnsAfterNextMessage(t *testing.T) {
	t.Parallel()

	listener := newSession(t)

	notifier, err := Dial(t.Context(), testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { notifier.Close() })

	if err := listener.Exec(t.Context(), "LISTEN testchan"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range Notifications(ctx, listener) {
			// Drain every notification; we only care that the
			// iterator returns after cancellation + next message.
		}
	}()

	// Cancel first; the iterator is blocked inside ReadHeader and
	// cannot observe the cancellation until another message arrives.
	cancel()

	// Kick the loop: one more message arrives, ReadHeader returns,
	// the loop's top-of-iteration ctx check sees the cancelled ctx
	// and Notifications returns.
	if err := notifier.Exec(t.Context(), "NOTIFY testchan, 'kick'"); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Notifications did not return within 5s after cancel + kick")
	}

	// The listener connection must still be usable after the iterator returns.
	if err := listener.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) after Notifications return: %v", err)
	}
}

// TestCopyFromCancelAtFinalize exercises the cancellation path inside
// finalizeCopyFrom. The ctx passed into Query is cancelled just before the
// caller breaks out of the Query range, so finalizeCopyFrom runs with a
// cancelled ctx. maybeRecoverFrom must then drain the response using
// context.Background(); otherwise the recovery would itself fail with
// context.Canceled and leave the connection in an unusable state.
func TestCopyFromCancelAtFinalize(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	if err := cn.Exec(t.Context(), "CREATE TEMP TABLE cancel_finalize (id INT, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	for row := range cn.Query(ctx, "COPY cancel_finalize FROM STDIN WITH (FORMAT binary)") {
		if _, err := row.Column(0).Bytes(); !errors.Is(err, ErrCopyFrom) {
			t.Fatalf("column error = %v, want ErrCopyFrom", err)
		}
		if err := cn.Exec(ctx, "", int32(1), "one"); err != nil {
			t.Fatal(err)
		}
		if err := cn.Exec(ctx, "", int32(2), "two"); err != nil {
			t.Fatal(err)
		}
		// Cancel right before leaving the range; finalizeCopyFrom
		// will execute with this cancelled ctx.
		cancel()
	}

	// The connection must be reusable after cancel-at-finalize.
	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) after cancel-at-finalize: %v", err)
	}

	// Characterization: the rows were written to the stream and a
	// CopyDone was sent before the recovery drain kicked in, so
	// PostgreSQL commits them. If that changes, update this count.
	// If finalize stopped using background ctx for recovery, the
	// Exec above would fail first and we would never get here.
	var count int64
	for row := range cn.Query(t.Context(), "SELECT count(*) FROM cancel_finalize") {
		n, err := row.Column(0).DecodeInt()
		if err != nil {
			t.Fatal(err)
		}
		count = n
	}
	if count != 2 {
		t.Logf("cancel_finalize row count = %d (characterization; update if semantics change)", count)
	}
}

// TestCopyToCancelMidStream cancels a COPY TO STDOUT after reading the
// first row. Many 'd' CopyData rows plus the trailer are still queued on
// the wire; the drain loop must read through all of them to reach
// ReadyForQuery so the connection is reusable.
func TestCopyToCancelMidStream(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const q = `COPY (SELECT * FROM generate_series(1, 5000)) TO STDOUT WITH (FORMAT binary)`

	rowsSeen := 0
	for row := range cn.Query(ctx, q) {
		v, err := row.Column(0).DecodeInt()
		if err != nil {
			// Cancellation may surface as a row-carried error
			// during the mid-stream drain; that's acceptable so
			// long as the connection recovers.
			if errors.Is(err, context.Canceled) {
				break
			}
			t.Fatal(err)
		}
		rowsSeen++
		if v != int64(rowsSeen) {
			t.Fatalf("row %d value = %d, want %d", rowsSeen, v, rowsSeen)
		}
		// After the first successful row, cancel and break. Many
		// more data rows + the 'c' trailer + 'C' CommandComplete +
		// 'Z' ReadyForQuery are still queued or inbound.
		cancel()
		break
	}
	if rowsSeen < 1 {
		t.Fatal("expected at least one row before cancelling")
	}

	// Status should land back at idle after the drain completes.
	if got := cn.fc.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("Status() after cancel-mid-stream = %q; want %q", got, febe.ReadyStatusIdle)
	}

	// Connection must be reusable.
	if err := cn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("Exec(SELECT 1) after cancel-mid-stream: %v", err)
	}
}
