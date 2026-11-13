package port

import (
	"context"

	"github.com/gopherex/pg-outbox/message"
)

// Instrumentation plugs telemetry into the outbox at its two ends.
//
// Capture runs at Enqueue, in the caller's context, before the insert. It
// fills headers, a fresh map merged into the message's headers afterwards:
// they are the only state it can carry to the relay, and they travel to the
// transport. Entries the caller set win over entries under the same key.
//
// Deliver runs in the relay for each claimed message just before Publish. It
// returns the context to publish that message with (derived from ctx, so the
// publish deadline and cancellation still apply) and an end func the relay
// calls exactly once with the message's Outcome, on every exit path.
type Instrumentation interface {
	Capture(ctx context.Context, headers map[string]string)
	Deliver(ctx context.Context, m message.Message) (context.Context, func(Outcome))
}

// Outcome is what became of one delivery attempt.
type Outcome struct {
	// Status is the status the attempt committed: published, pending (a
	// retry is scheduled), dead or discarded; processing when the attempt
	// left the row claimed (abort, panic, lost lease, failed completion).
	Status message.Status
	// Err is the publish error or the cause; nil when published.
	Err error
}

// NoopInstrumentation is the default Instrumentation: it adds no headers and
// publishes every message with the batch context.
type NoopInstrumentation struct{}

// Capture adds nothing.
func (NoopInstrumentation) Capture(context.Context, map[string]string) {}

// Deliver returns ctx and an end func that does nothing.
func (NoopInstrumentation) Deliver(ctx context.Context, _ message.Message) (context.Context, func(Outcome)) {
	return ctx, func(Outcome) {}
}

type messageContextsKey struct{}

// WithMessageContexts returns ctx carrying the per-message contexts the relay
// opened with Instrumentation.Deliver, keyed by message id.
func WithMessageContexts(ctx context.Context, byID map[string]context.Context) context.Context {
	return context.WithValue(ctx, messageContextsKey{}, byID)
}

// MessageContext returns the context Instrumentation.Deliver opened for m, or
// ctx itself when there is none. A Publisher that sends messages one by one
// uses it so the transport client continues m's delivery.
func MessageContext(ctx context.Context, m message.Message) context.Context {
	byID, _ := ctx.Value(messageContextsKey{}).(map[string]context.Context)
	if mctx, ok := byID[m.ID]; ok {
		return mctx
	}
	return ctx
}
