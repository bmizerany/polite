package febe

import (
	"cmp"
	"context"
)

type Parameter struct {
	Name  string
	Value string
}

type Trace struct {
	ReceivedHeader          func(context.Context, MessageHeader)
	ReceivedNotice          func(context.Context, *Notice)
	ReceivedError           func(context.Context, *Error)
	ReceivedNotification    func(context.Context, *Notification)
	ReceivedParameterStatus func(context.Context, Parameter)
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
