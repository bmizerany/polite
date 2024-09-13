// Package testutil provides utilities for testing with a focus on catching
// common mistakes, such as using non-idiomatic logging.
//
// It also aims to surface errors that may otherwise be ignored, such as
// logging errors post writting HTTP headers.
package testutil

import (
	"context"
	"log/slog"
	"testing"
)

type logWriter struct {
	t testing.TB
}

func (w logWriter) Write(b []byte) (n int, err error) {
	w.t.Logf("%s", b)
	return len(b), nil
}

type fatalOnErrHandler struct {
	t testing.TB
	slog.Handler
}

func (h fatalOnErrHandler) Handle(ctx context.Context, r slog.Record) error {
	h.t.Helper()
	for a := range r.Attrs {
		if a.Key == "error" {
			h.t.Fatal(`logged key "error" should be "err"`)
		}
		if a.Key == "err" {
			h.Handler.Handle(ctx, r)
			h.t.Fatalf("logged ERROR: %v", a.Value)
		}
	}
	return h.Handler.Handle(ctx, r)
}

// Slooger returns a logger that logs to t.Logf and fails the test if it logs
// an error, no matter the log level. Typically user errors are logged as
// warnings or debug, but we want to surface them in tests.
func Slogger(t testing.TB) *slog.Logger {
	return slog.New(fatalOnErrHandler{t,
		slog.NewTextHandler(logWriter{t}, nil)})
}

type nopLogWriter struct{}

func (nopLogWriter) Write(b []byte) (n int, err error) {
	return len(b), nil
}

// SloggerNop returns a logger that logs nothing. It is useful for benchmarking
// to prevent t.Logf from showing up in profiles.
//
// The slog.TextHandler is used.
func SloggerNop() *slog.Logger {
	return slog.New(slog.NewTextHandler(nopLogWriter{}, nil))
}
