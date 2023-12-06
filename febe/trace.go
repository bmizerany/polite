package febe

import (
	"cmp"
	"context"
)

type Trace struct {
	ReceivedHeader          func(context.Context, MessageHeader)
	ReceivedNotice          func(context.Context, *Notice)
	ReceivedError           func(context.Context, *Error)
	ReceivedParameterStatus func(context.Context, ParameterStatusResponse)

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
