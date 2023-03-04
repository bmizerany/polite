package febe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestError(t *testing.T) {
	c := New(nil, errorWriter{}) // panics if a Read is called
	for i := 0; i < 3; i++ {
		err := c.WriteStartupMessage(context.Background())
		if i == 0 && err != nil {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		} else if i > 0 && !errors.Is(err, errTest) {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		}

		if !errors.Is(c.Flush(), errTest) {
			t.Fatalf("[%d]: unexpected error: %v", i, err)
		}
	}
}

func TestStartupResponse(t *testing.T) {
	var b bytes.Buffer

	ctx := context.Background()
	c := New(&b, &b)
	c.WriteStartupMessage(ctx, "user", "test")
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}
}

var errTest = errors.New("febe_test: test error")

// errorWriter is an io.Writer that always returns an error.
type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("errorWriter.Write: %w", errTest)
}
