package port

import (
	"context"
	"sync"
	"time"

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
// returns the context to publish that message with (derived from ctx, so
// cancellation still applies) and an end func the relay
// calls exactly once with the message's Outcome, on every exit path. The
// relay then runs Capture on that context and lays the result over the
// headers of the copy handed to Publish (these entries win; the stored row is
// unchanged), so a transport mapping Message.Headers carries the delivery on.
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

// messageContexts are the contexts of one batch's messages. With a timeout,
// a message's context gets its deadline when the publisher first asks for
// it, so every message has the whole budget however long the messages
// before it took.
type messageContexts struct {
	byID    map[string]context.Context
	timeout time.Duration

	mu      sync.Mutex
	opened  map[string]context.Context
	cancels []context.CancelFunc
}

func (c *messageContexts) open(id string) (context.Context, bool) {
	mctx, ok := c.byID[id]
	if !ok || c.timeout <= 0 {
		return mctx, ok
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if opened, ok := c.opened[id]; ok {
		return opened, true
	}
	opened, cancel := context.WithTimeout(mctx, c.timeout)
	c.opened[id], c.cancels = opened, append(c.cancels, cancel)
	return opened, true
}

func (c *messageContexts) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cancel := range c.cancels {
		cancel()
	}
	c.cancels = nil
}

// WithMessageContexts returns ctx carrying the per-message contexts the relay
// opened with Instrumentation.Deliver, keyed by message id.
func WithMessageContexts(ctx context.Context, byID map[string]context.Context) context.Context {
	return context.WithValue(ctx, messageContextsKey{}, &messageContexts{byID: byID})
}

// WithMessageTimeouts is WithMessageContexts with a publish budget for each
// message: the context MessageContext returns for a message ends timeout
// after the publisher first asks for it, so a slow message never eats the
// budget of the ones sent after it. A zero timeout adds no deadline. release
// stops the timers; call it once Publish has returned.
func WithMessageTimeouts(
	ctx context.Context, byID map[string]context.Context, timeout time.Duration,
) (_ context.Context, release func()) {
	c := &messageContexts{byID: byID, timeout: timeout, opened: make(map[string]context.Context, len(byID))}
	return context.WithValue(ctx, messageContextsKey{}, c), c.release
}

// MessageContext returns the context Instrumentation.Deliver opened for m,
// with m's publish budget when the relay sets one, or ctx itself when there
// is none. A Publisher that sends messages one by one uses it for each
// message, right when it starts sending it, so the transport client
// continues m's delivery and m gets its own deadline.
func MessageContext(ctx context.Context, m message.Message) context.Context {
	c, _ := ctx.Value(messageContextsKey{}).(*messageContexts)
	if c == nil {
		return ctx
	}
	if mctx, ok := c.open(m.ID); ok {
		return mctx
	}
	return ctx
}
