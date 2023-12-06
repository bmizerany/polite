package postgres

import (
	"cmp"
	"context"

	"blake.io/postgres/febe"
)

type Trace struct {
	ReceivedNotice func(ctx context.Context, err *febe.Notice)
	ReceivedError  func(ctx context.Context, err *febe.Error)

	WroteExecute func(ctx context.Context, portal, statement string, args []string)
}

type traceContextKey struct{}

var traceKey traceContextKey

func WithTrace(ctx context.Context, trace *Trace) context.Context {
	if trace == nil {
		// maintain the invariant that if ctx.Value(traceKey)
		// returns ok true, then trace is not nil
		return ctx
	}
	return context.WithValue(ctx, traceKey, trace)
}

var emptyTrace = &Trace{}

func traceFromContext(ctx context.Context) *Trace {
	trace, _ := ctx.Value(traceKey).(*Trace)
	return cmp.Or(trace, emptyTrace)
}
