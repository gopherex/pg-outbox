# pg-outbox

Transactional outbox for PostgreSQL. Transport-agnostic, horizontally scalable.

The database surface is the `Executor` interface (`Exec` / `Query` / `QueryRow`).
`*pgxpool.Pool`, `pgx.Tx`, `*pgx.Conn` and pgtx's `tx.DB` all satisfy it, so you
can plug your own pooling / routing / instrumentation layer.

## Install

```bash
go get github.com/gopherex/pg-outbox
```

## Migrate

The SQL is schema-unqualified. Apply it with your tool of choice via
`outbox.MigrationsFS()` (an `embed.FS`), or apply the raw statements from
`outbox.Migrations()`:

```go
for _, stmt := range outbox.Migrations() {
    if _, err := pool.Exec(ctx, stmt); err != nil { log.Fatal(err) }
}
```

To install into a non-default schema, set `search_path` on the migration
connection; then pass the same schema to `WithSchema`.

## Use

```go
// exec drives the background relay (must NOT be tx-bound);
// enq is pgtx's tx.DB (so Enqueue joins your business transaction).
ob, err := outbox.New(pool, enq, myPublisher,
    outbox.WithInstanceID(os.Getenv("POD_NAME")), // unique, stable per process
    outbox.WithSchema("public"),
    outbox.WithConcurrency(4),
    outbox.WithRetention(72*time.Hour),
    outbox.WithCleanupInterval(time.Hour),
)
if err != nil { log.Fatal(err) }

// Inside a pgtx transaction, atomically with your business writes:
err = tx.DoSerializable(ctx, mgr, func(ctx context.Context) error {
    if err := repo.Save(ctx, order); err != nil { return err }
    return ob.Enqueue(ctx, outbox.Message{
        Topic:        "orders",
        PartitionKey: order.ID,
        Payload:      data,
        Headers:      map[string]string{"trace-id": traceID},
    })
})

// In a background goroutine:
go ob.Run(ctx) // blocks until ctx is cancelled, then drains and returns
```

Implement `outbox.Publisher` for your transport:

```go
type Publisher interface {
    Publish(ctx context.Context, msgs []outbox.Message) error
}
```

Plug your own database layer by implementing `outbox.Executor`:

```go
type Executor interface {
    Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
    Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
    QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
```

`LISTEN/NOTIFY` wake-ups need a `*pgxpool.Pool`: supply one with
`WithListenPool`, or pass a `*pgxpool.Pool` as the relay executor and it is
detected automatically. Without one the relay falls back to polling.

## Configuration

Two interchangeable styles, usable together (functional options win):

```go
// declarative — mapstructure/validate/default tags, load from file or env:
var cfg outbox.Config // see config.Config fields
ob, _ := outbox.New(pool, enq, pub,
    outbox.WithConfig(cfg),
    outbox.WithLogger(log), // logger injected separately, not config data
)

// or pure options:
ob, _ := outbox.New(pool, enq, pub, outbox.WithInstanceID("pod-1"), ...)
```

## Logging

The logger is the standard library's `*slog.Logger`, injected via
`WithLogger`. Default is a no-op (`slog.DiscardHandler`), so nothing is logged
until you supply one — no third-party logging dependency:

```go
ob, _ := outbox.New(pool, enq, pub, outbox.WithLogger(slog.Default()))
```

## Retry backoff

Default is exponential 100ms→30s with jitter. Override with `WithRetryBackoff`.
The `Backoff` type is `func(attempt int) time.Duration` (stateless, indexed by
the message's persisted attempt count — survives restarts). Adapters bridge
popular libraries without adding them as dependencies:

```go
// github.com/cenkalti/backoff
outbox.WithRetryBackoff(backoff.FromNextBackOff(func() backoff.NextBackOffer {
    return cbackoff.NewExponentialBackOff()
}))

// github.com/sethvargo/go-retry
outbox.WithRetryBackoff(backoff.FromNexter(func() backoff.Nexter { ... }))
```

## Package layout

The root package `outbox` is the only public surface (facade + re-exports). The
implementation lives in subpackages: `message`, `port` (Publisher/Codec/Hooks/
Executor), `config`, `backoff`, `migrations`, `engine` (store/relay/cleaner).
The published module depends only on `pgx`; the standard library covers logging.

## Semantics

- **At-least-once.** A failed batch is retried in full; consumers must
  deduplicate (e.g. on `Message.ID`).
- **Scaling.** Run one relay per instance with a unique `WithInstanceID`.
  Claims use `FOR UPDATE SKIP LOCKED` + a lease (`locked_until`); a crashed
  instance's rows are re-claimed once its lease expires.
- **Retries / dead-letter.** On failure `attempts` increments and the row is
  rescheduled with backoff; after `WithMaxAttempts` it becomes `dead` and stays
  in the table for inspection.
- **Ordering.** Best-effort by `created_at`. `WithOrdered(true)` enforces strict
  per-`PartitionKey` ordering; note a `dead` row then blocks its key until an
  operator resolves it.
- **Latency.** A trigger fires `NOTIFY`; the relay wakes near-instantly and
  falls back to polling every `WithPollInterval`.

## Notes

- `attempts` increments at claim time, so a crash mid-publish counts as an
  attempt (poison-message protection).
- The table name is fixed (`outbox_messages`); only the schema is configurable.
- Codecs (JSON/proto) and concrete publishers (Kafka/NATS/…) are out of scope —
  bring your own.

## Tests

```bash
go test -race ./...                       # pure-logic unit tests, no DB
cd test/integration && go test ./...      # black-box integration (testcontainers Postgres, Docker required)
```

Integration tests live in a **separate nested module** (`test/integration`) so
the heavy testcontainers / docker dependency tree never enters this module's
`go.mod`. The published library stays a single `pgx` dependency.
