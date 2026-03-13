// Package testutil provides utilities for testing with a focus on catching
// common mistakes, such as using non-idiomatic logging.
//
// It also aims to surface errors that may otherwise be ignored, such as
// logging errors post writting HTTP headers.
package testutil

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

type logWriter struct {
	t testing.TB
}

func (w logWriter) Write(b []byte) (n int, err error) {
	w.t.Helper()
	w.t.Logf("%s", b)
	return len(b), nil
}

type failOnERRORHandler struct {
	t testing.TB
	slog.Handler
}

func (h failOnERRORHandler) Handle(ctx context.Context, r slog.Record) error {
	h.t.Helper()
	if r.Level == slog.LevelError {
		h.t.Errorf("unexpected error: %s", r.Message)
	}
	return h.Handler.Handle(ctx, r)
}

var handlerOptions = &slog.HandlerOptions{
	Level: slog.LevelDebug,
}

// SloggerFail returns a logger suitable for use in tests. It logs to t.Logf.
// Unlike, [Slogger] it will log all ERROR logs to t.Error.
func SloggerFail(t testing.TB) *slog.Logger {
	t.Helper()
	return slog.New(failOnERRORHandler{t, slog.NewTextHandler(logWriter{t}, handlerOptions)})
}

// Slogger returns a logger suitable for use in tests. It logs to t.Logf and
// will fail if the log level is ERROR.
func Slogger(t testing.TB) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(logWriter{t}, handlerOptions))
}

// Checker returns a function that can be used to check that err wraps or
// equals want and logs a test error if it does not.
func Checker(t testing.TB) func(err, want error) {
	t.Helper()
	return func(got, want error) {
		t.Helper()
		if !errors.Is(got, want) {
			t.Fatalf("err[%T] = %v, want %v", got, got, want)
		}
	}
}
