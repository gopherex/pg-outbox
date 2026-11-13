// Package outboxtrace traces pg-outbox deliveries with gopherex/xtrace.
//
// It implements outbox.Instrumentation: Capture writes the enqueuing span's
// W3C trace context into the message headers, and Deliver starts each
// delivery as a new root span linked to that origin — an asynchronous
// continuation gets its own trace, findable from the command that enqueued it.
//
//	ob, err := outbox.New(pool, enq, pub, outbox.WithInstrumentation(outboxtrace.New()))
package outboxtrace

import (
	"context"
	"fmt"

	outbox "github.com/gopherex/pg-outbox"
	"github.com/gopherex/xtrace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the default instrumentation scope.
const ScopeName = "github.com/gopherex/pg-outbox"

// SpanName names every delivery span.
const SpanName = "outbox deliver"

// Option configures New.
type Option func(*Instrumentation)

// WithScope sets the scope whose tracer starts delivery spans (default
// xtrace.New(ScopeName) on the global tracer provider). The scope's error
// renderer (xtrace.WithErrorRenderer) renders a failed delivery's cause.
func WithScope(s *xtrace.Scope) Option {
	return func(i *Instrumentation) { i.tracer, i.render = s.Tracer(), s.ErrorRenderer() }
}

// WithPropagator sets the propagator that writes and reads the headers
// (default otel.GetTextMapPropagator()).
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(i *Instrumentation) { i.propagator = p }
}

// Instrumentation is the xtrace implementation of outbox.Instrumentation.
type Instrumentation struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	render     func(error) string // nil: the cause is recorded as is
}

var _ outbox.Instrumentation = (*Instrumentation)(nil)

// New builds the instrumentation.
func New(opts ...Option) *Instrumentation {
	i := &Instrumentation{}
	for _, opt := range opts {
		opt(i)
	}
	if i.tracer == nil {
		i.tracer = xtrace.New(ScopeName).Tracer()
	}
	if i.propagator == nil {
		i.propagator = otel.GetTextMapPropagator()
	}
	return i
}

// Capture writes the trace context of ctx's span into headers; nothing when
// ctx carries none.
func (i *Instrumentation) Capture(ctx context.Context, headers map[string]string) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return
	}
	i.propagator.Inject(ctx, propagation.MapCarrier(headers))
}

// recordError writes the exception event, with the cause rendered when the
// scope has a renderer (RecordError would write err.Error()).
func (i *Instrumentation) recordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	if i.render == nil {
		span.RecordError(err)
		return
	}
	span.AddEvent("exception", trace.WithAttributes(
		attribute.String("exception.type", fmt.Sprintf("%T", err)),
		attribute.String("exception.message", i.render(err))))
}

// Deliver starts the delivery span of m: a new root, of kind producer, linked
// to the origin captured in m's headers when there is one. Attributes are
// bounded: the topic, the attempt number and, at the end, the outcome.
func (i *Instrumentation) Deliver(ctx context.Context, m outbox.Message) (context.Context, func(outbox.Outcome)) {
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.destination.name", m.Topic),
			attribute.Int("outbox.attempt", m.Attempts),
		),
	}
	origin := trace.SpanContextFromContext(i.propagator.Extract(context.Background(), propagation.MapCarrier(m.Headers)))
	if origin.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: origin}))
	}
	ctx, span := i.tracer.Start(ctx, SpanName, opts...)
	return ctx, func(o outbox.Outcome) {
		span.SetAttributes(attribute.String("outbox.outcome", string(o.Status)))
		// A scheduled retry and a discard are expected refusals, not failures.
		if o.Status == outbox.StatusDead || o.Status == outbox.StatusProcessing {
			i.recordError(span, o.Err)
			span.SetStatus(codes.Error, string(o.Status))
		}
		span.End()
	}
}
