package integration

import (
	"context"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
	outboxtrace "github.com/gopherex/pg-outbox/contrib/libs/xtrace"
	"github.com/gopherex/xtrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// A command's span is the origin of its messages' deliveries: the relay
// delivers each under a new trace linked to it, and the publisher sees the
// delivery span through MessageContext.
func TestTraceLinksDeliveryToCommand(t *testing.T) {
	pool := newTestPool(t)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	published := make(chan trace.SpanContext, 1)
	pub := publisherFunc(func(ctx context.Context, ms []outbox.Message) error {
		published <- trace.SpanContextFromContext(outbox.MessageContext(ctx, ms[0]))
		return nil
	})
	ob, err := outbox.New(pool, pool, pub, outbox.WithPollInterval(10*time.Millisecond),
		outbox.WithInstrumentation(outboxtrace.New(
			outboxtrace.WithScope(xtrace.New("test", xtrace.WithTracerProvider(tp))),
			outboxtrace.WithPropagator(propagation.TraceContext{}))))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmdCtx, command := tp.Tracer("app").Start(ctx, "command")
	if err := ob.Enqueue(cmdCtx, outbox.Message{Topic: "orders", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	command.End()
	go func() { _ = ob.Run(ctx) }()

	var delivery trace.SpanContext
	select {
	case delivery = <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("not delivered")
	}
	if !delivery.IsValid() || delivery.TraceID() == command.SpanContext().TraceID() {
		t.Fatalf("publisher saw %v, want a delivery span in a new trace", delivery)
	}
	waitFor(t, 5*time.Second, func() bool { return len(exp.GetSpans()) == 2 })
	for _, s := range exp.GetSpans() {
		if s.SpanContext.SpanID() != delivery.SpanID() {
			continue
		}
		if len(s.Links) != 1 || s.Links[0].SpanContext.SpanID() != command.SpanContext().SpanID() {
			t.Fatalf("delivery links %v, want the command span", s.Links)
		}
		return
	}
	t.Fatal("delivery span not exported")
}
