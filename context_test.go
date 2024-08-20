package pqlite

import (
	"context"
	"testing"
	"time"
)

// TestDeadlineIsolation tests that the SetReadDeadline and/or SetWriteDeadline
// of one action does not affect the actions that come after it.
func TestDeadlineIsolation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c, err := Open(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}

	cancel()

	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("unexpected error after context cancel: %v", err)
	}
}
