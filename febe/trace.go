package febe

import (
	"cmp"
	"context"
)

// Parameter is a name/value pair from a ParameterStatus message.
type Parameter struct {
	// Name is the parameter name.
	Name string

	// Value is the parameter value.
	Value string
}

// Trace holds callbacks invoked for selected backend messages.
// Each callback runs after the message has been decoded.
// Nil callbacks are ignored.
// The zero Trace does nothing.
type Trace struct {
	// ReceivedHeader is called for each backend message header.
	ReceivedHeader func(context.Context, MessageHeader)

	// ReceivedNotice is called for each Notice response.
	ReceivedNotice func(context.Context, *Notice)

	// ReceivedError is called for each Error response.
	ReceivedError func(context.Context, *Error)

	// ReceivedNotification is called for each Notification response.
	ReceivedNotification func(context.Context, *Notification)

	// ReceivedParameterStatus is called for each ParameterStatus response.
	ReceivedParameterStatus func(context.Context, Parameter)

	// ObservedError is called for non-message errors observed while
	// processing protocol traffic.
	ObservedError func(context.Context, error)
}

type traceContextKey struct{}

var traceKey traceContextKey

// WithTrace returns a copy of ctx that carries trace.
// If trace is nil, WithTrace returns ctx unchanged.
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

// TraceObservedError reports err to the trace attached to ctx, if any.
func TraceObservedError(ctx context.Context, err error) {
	if err == nil {
		return
	}
	if trace := traceFromContext(ctx); trace.ObservedError != nil {
		trace.ObservedError(ctx, err)
	}
}
