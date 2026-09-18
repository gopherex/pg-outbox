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

Use a contrib publisher:

```go
import outboxnats "github.com/gopherex/pg-outbox/contrib/publishers/nats"

pub := outboxnats.New(natsConn)
ob, err := outbox.New(pool, enq, pub)
```

Available publisher contrib modules:

- `github.com/gopherex/pg-outbox/contrib/publishers/nats`
- `github.com/gopherex/pg-outbox/contrib/publishers/kafka`
- `github.com/gopherex/pg-outbox/contrib/publishers/valkey`

Or implement `outbox.Publisher` for your own transport:

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

## Delivery control

Existing publishers remain compatible: `nil` means published; an ordinary
error retries the entire batch with the configured backoff and attempt limit.
A publisher can return these wrapped outcomes (recognized through `errors.As`):

```go
return outbox.Permanent(err)                  // dead immediately
return outbox.RetryAfter(err, 2*time.Minute)   // minimum delay; attempt limit still applies
return outbox.Discard(reason)                 // terminal, without publication or dead-letter
```

These outcomes apply to **every message in the batch**. Use `WithBatchSize(1)`
for independent outcomes and `WithConcurrency` for parallelism. RetryAfter
uses the greater of its delay and the configured backoff; zero/negative delays
never reduce backoff. Wrappers preserve `errors.Is` / `errors.As`. Do not mix
outcome markers; if nested/joined, Permanent takes precedence over Discard,
then RetryAfter. Nil causes still produce the requested outcome.

The library treats payloads as opaque bytes. Application policy, deadlines,
provider error classification, recipient preferences, and authorization belong
in the caller/publisher. Store only sanitized errors: error text is persisted
in `last_error` and passed to observability hooks.

### Claims, cancellation, and completion

Every claim gets a fresh `Message.LeaseToken`. Completion and renewal require
that token **and an unexpired processing lease**. A stale worker cannot finish,
retry, or dead-letter a later attempt, even with the same instance ID or after
an operator resets attempts. `ErrLeaseLost` is observable through the relay's
error logger; stale completions do not emit outcome hooks.

While Publish runs, the relay renews the batch every third of LeaseDuration.
A renewal failure cancels the publisher context and leaves recovery to lease
expiry. Publishers must honor cancellation and configure transport timeouts;
`WithPublishTimeout` optionally bounds the context for each Publish call
(disabled by default). Cancellation cannot undo an external side effect, and
an adapter that ignores context cannot be forcibly stopped. Delivery remains
at-least-once, including when publication succeeds but recording it fails.

Hooks run after database transitions and receive the resulting row snapshots.
They are best-effort observations, not a transaction or a durable event stream.
An optional `DiscardHooks` interface adds `OnDiscarded(ctx, msgs, reason)`;
existing Hooks implementations do not need to change. Discards never call
OnPublished or OnDead. Process termination between commit and a hook can lose
that observation.

### Terminal data and replay

```go
outbox.WithClearPayloadOnComplete(true) // empty payload + headers on published/discarded
outbox.WithRetention(24*time.Hour)       // delete published rows
outbox.WithDeadRetention(7*24*time.Hour) // delete dead rows
outbox.WithDiscardRetention(time.Hour)  // delete discarded rows
outbox.WithCleanupInterval(time.Minute)
```

All data-clearing/deletion policies are **opt-in**. Payload and headers are
cleared atomically with the successful/discarded transition; dead rows retain
them for investigation and replay. Hooks see the cleared fields when enabled.
Other columns, including topic, partition key, and last_error, are not scrubbed.
SQL deletion/clearing does not erase historical WAL, backups, or caller logs.

Dead/discarded retention starts at `finished_at`, published retention at
`published_at`. Cleanup locks selected rows and skips locks, preventing it
from deleting a concurrently requeued job. Pending/processing rows are never
removed by the cleaner.

`ob.Requeue(ctx, id)` returns `(changed bool, err error)`. Only dead rows qualify;
it resets attempts and terminal/lease state and emits a transactional wake-up.
It uses the **enqueue executor**, so the caller can validate application policy
and requeue in the same transaction. It preserves ID, creation time, and data;
the next attempt receives a new lease token. Replaying a job can repeat an
external side effect. The caller owns permission and eligibility checks.

### Upgrade from v1.0

Apply `0002_delivery_control.sql` before using the new relay. It adds claim
tokens, terminal timestamps, and the generic `discarded` state. Existing data
is preserved; historical dead rows begin their retention window at migration
time because their original terminal time is unknown. New retention settings
remain disabled unless configured.

Stop/drain old relays before starting new ones. Old binaries do not fence their
completion writes; do not mix old and new relays against the same queue.
The Publisher, Hooks, and Executor interfaces remain source-compatible.

## Package layout

The root package `outbox` is the core public surface (facade + re-exports). The
implementation lives in subpackages: `message`, `port` (Publisher/Codec/Hooks/
Executor), `config`, `backoff`, `migrations`, `engine` (store/relay/cleaner).
Concrete publishers live in nested `contrib/publishers/*` modules so Kafka,
NATS and Valkey dependencies never enter the core module's `go.mod`.

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
  per-`PartitionKey` ordering (creation time, then ID); a `dead` row blocks its
  key until resolved or removed. Published/discarded rows do not block it.
- **Latency.** A trigger fires `NOTIFY`; the relay wakes near-instantly and
  falls back to polling every `WithPollInterval`.

## Notes

- `attempts` increments at claim time, so a crash mid-publish counts as an
  attempt (poison-message protection).
- The table name is fixed (`outbox_messages`); only the schema is configurable.
- Codecs (JSON/proto) are out of scope — bring your own.

## Tests

```bash
make test                                 # root + contrib module tests, no DB
cd test/integration && go test ./...      # black-box integration (testcontainers Postgres, Docker required)
```

Contrib publishers and integration tests live in **separate nested modules** so
their dependency trees never enter the core module's `go.mod`. The core library
stays a single `pgx` dependency.
