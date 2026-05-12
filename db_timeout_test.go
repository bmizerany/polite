package polite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestQueryDeadlineAllowsReuse(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	gotErr := func() error {
		for row := range cn.Query(ctx, "SELECT pg_sleep(1)") {
			if _, err := row.Column(0).Bytes(); err != nil {
				return err
			}
		}
		return nil
	}()
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
