# outboxtrace

Tracing for [pg-outbox](../../../README.md) deliveries with
[gopherex/xtrace](https://github.com/gopherex/xtrace): an implementation of
`outbox.Instrumentation`.

```bash
go get github.com/gopherex/pg-outbox/contrib/libs/xtrace@latest
```

```go
ob, err := outbox.New(pool, enq, pub,
    outbox.WithInstrumentation(outboxtrace.New()), // global tracer provider and propagator
)
```

- **Enqueue** under a span writes its W3C trace context (`traceparent`,
  `tracestate`) into the message headers. They are stored with the row and
  sent to the transport. Without a span nothing is written.
- **Delivery** of each message starts a new root span `outbox deliver` (kind
  producer) with a link to that origin, so the command's trace leads to its
  deliveries and every delivery is its own trace. Without an origin there is
  no link.
- The publisher gets the delivery span with `outbox.MessageContext(ctx, m)`;
  the built-in Valkey publishers send each message under it.
- Attributes are bounded: `messaging.destination.name` (topic),
  `outbox.attempt`, `outbox.outcome` (`published`, `pending`, `dead`,
  `discarded`, `processing`). A scheduled retry and a discard are expected
  refusals and leave the span unmarked; a dead row or an attempt left claimed
  records the error and sets the span status to Error.

Options: `WithScope(xtrace.New(...))` for the tracer (default scope
`github.com/gopherex/pg-outbox` on the global provider),
`WithPropagator(p)` (default `otel.GetTextMapPropagator()`).

## Tests

```bash
go test ./...                      # in-memory exporter, no database
cd ../../../test/integration && go test -run TraceLinks ./...   # end to end on Postgres
```
