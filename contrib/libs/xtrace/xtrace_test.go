package outboxtrace

import (
	"context"
	"errors"
	"strings"
	"testing"

	outbox "github.com/gopherex/pg-outbox"
	"github.com/gopherex/xtrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func setup(t *testing.T) (outbox.Instrumentation, trace.Tracer, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	inst := New(WithScope(xtrace.New("test", xtrace.WithTracerProvider(tp))),
		WithPropagator(propagation.TraceContext{}))
	return inst, tp.Tracer("caller"), exp
}

func attr(span tracetest.SpanStub, key attribute.Key) attribute.Value {
	for _, kv := range span.Attributes {
		if kv.Key == key {
			return kv.Value
		}
	}
	return attribute.Value{}
}

// A message enqueued under a span is delivered under a new root span linked
// to it, and the delivery context carries that span.
func TestDeliveryLinksToOrigin(t *testing.T) {
	t.Parallel()
	inst, tracer, exp := setup(t)

	ctx, origin := tracer.Start(context.Background(), "command")
	headers := map[string]string{}
	inst.Capture(ctx, headers)
	origin.End()
	if headers["traceparent"] == "" {
		t.Fatalf("Capture wrote no traceparent: %v", headers)
	}

	dctx, end := inst.Deliver(context.Background(), outbox.Message{ID: "a", Topic: "orders", Attempts: 2, Headers: headers})
	if got := trace.SpanContextFromContext(dctx); !got.IsValid() || got.TraceID() == origin.SpanContext().TraceID() {
		t.Fatalf("delivery context %v: want a valid span in a new trace", got)
	}
	end(outbox.Outcome{Status: outbox.StatusPublished})

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("%d spans, want origin + delivery", len(spans))
	}
	d := spans[1]
	if d.Parent.IsValid() {
		t.Fatalf("delivery has a parent %v, want a root", d.Parent)
	}
	if len(d.Links) != 1 || d.Links[0].SpanContext.SpanID() != origin.SpanContext().SpanID() {
		t.Fatalf("delivery links %v, want the origin", d.Links)
	}
	if d.SpanKind != trace.SpanKindProducer || attr(d, "messaging.destination.name").AsString() != "orders" ||
		attr(d, "outbox.attempt").AsInt64() != 2 || attr(d, "outbox.outcome").AsString() != "published" ||
		d.Status.Code == codes.Error {
		t.Fatalf("delivery span %s: kind %v attrs %v status %v", d.Name, d.SpanKind, d.Attributes, d.Status)
	}
}

// Without an origin there is nothing to link and nothing to capture.
func TestDeliveryWithoutOrigin(t *testing.T) {
	t.Parallel()
	inst, _, exp := setup(t)
	headers := map[string]string{}
	inst.Capture(context.Background(), headers)
	if len(headers) != 0 {
		t.Fatalf("Capture without a span wrote %v", headers)
	}
	_, end := inst.Deliver(context.Background(), outbox.Message{ID: "a", Topic: "t"})
	end(outbox.Outcome{Status: outbox.StatusPublished})
	if d := exp.GetSpans()[0]; len(d.Links) != 0 || d.Parent.IsValid() {
		t.Fatalf("links %v parent %v, want neither", d.Links, d.Parent)
	}
}

// Expected refusals leave the span unmarked; a dead row or an attempt left
// claimed is a technical failure.
func TestDeliveryOutcomeStatus(t *testing.T) {
	t.Parallel()
	errCause := errors.New("cause")
	for _, tc := range []struct {
		status outbox.Status
		failed bool
	}{
		{status: outbox.StatusPublished},
		{status: outbox.StatusPending},
		{status: outbox.StatusDiscarded},
		{status: outbox.StatusDead, failed: true},
		{status: outbox.StatusProcessing, failed: true},
	} {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()
			inst, _, exp := setup(t)
			_, end := inst.Deliver(context.Background(), outbox.Message{ID: "a", Topic: "t"})
			var err error
			if tc.status != outbox.StatusPublished {
				err = errCause
			}
			end(outbox.Outcome{Status: tc.status, Err: err})
			d := exp.GetSpans()[0]
			if (d.Status.Code == codes.Error) != tc.failed || (len(d.Events) > 0) != tc.failed {
				t.Fatalf("status %v events %v, want failed=%v", d.Status, d.Events, tc.failed)
			}
			if got := attr(d, "outbox.outcome").AsString(); got != string(tc.status) {
				t.Fatalf("outcome %q, want %q", got, tc.status)
			}
		})
	}
}

// A scope's error renderer keeps the raw cause out of the delivery span.
func TestDeliveryRendersErrors(t *testing.T) {
	t.Parallel()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	inst := New(WithScope(xtrace.New("test", xtrace.WithTracerProvider(tp),
		xtrace.WithErrorRenderer(func(error) string { return "rendered" }))))

	_, end := inst.Deliver(context.Background(), outbox.Message{ID: "a", Topic: "t"})
	end(outbox.Outcome{Status: outbox.StatusDead, Err: errors.New("GET https://h/p?token=secret")})

	d := exp.GetSpans()[0]
	if len(d.Events) != 1 || d.Status.Code != codes.Error {
		t.Fatalf("events %v status %v", d.Events, d.Status)
	}
	for _, a := range d.Events[0].Attributes {
		if strings.Contains(a.Value.Emit(), "secret") {
			t.Fatalf("raw cause leaked: %v", d.Events[0].Attributes)
		}
		if a.Key == "exception.message" && a.Value.AsString() != "rendered" {
			t.Fatalf("exception.message = %q", a.Value.AsString())
		}
	}
}

func BenchmarkDeliver(b *testing.B) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	inst := New(WithScope(xtrace.New("bench", xtrace.WithTracerProvider(tp))), WithPropagator(propagation.TraceContext{}))
	ctx, span := tp.Tracer("caller").Start(context.Background(), "command")
	headers := map[string]string{}
	inst.Capture(ctx, headers)
	span.End()
	m := outbox.Message{ID: "a", Topic: "t", Headers: headers}
	for b.Loop() {
		_, end := inst.Deliver(context.Background(), m)
		end(outbox.Outcome{Status: outbox.StatusPublished})
		exp.Reset()
	}
}
