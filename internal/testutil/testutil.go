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

// Slooger returns a logger suitable for use in tests. It logs to t.Logf and
// will fail if the log level is ERROR.
func Slogger(t testing.TB) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(logWriter{t}, handlerOptions))
}
