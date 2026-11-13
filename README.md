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

// Under the service lifecycle: blocks until ctx is cancelled (returns ctx.Err())
// or until a fatal error stops the relay (returns that error).
err = ob.Run(ctx)
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
return outbox.Abort(err)                      // stop the relay; batch left claimed
```

These outcomes apply to **every message in the batch**. RetryAfter uses the
greater of its delay and the configured backoff; zero/negative delays never
reduce backoff. Wrappers preserve `errors.Is` / `errors.As`. Do not mix
outcome markers; if nested/joined, Abort takes precedence over PerMessage, then
Permanent, then Discard, then RetryAfter. Nil causes still produce the
requested outcome.

For a batch of unrelated messages, give each its own outcome, keyed by
`Message.ID`:

```go
return outbox.PerMessage(map[string]error{
    a.ID: outbox.Permanent(err),           // only a is dead
    c.ID: outbox.RetryAfter(err, time.Minute),
}, nil)                                    // every other message: published
```

A `nil` entry means published; a non-nil entry is settled exactly like the
whole-batch outcomes above (a plain error retries with backoff). Messages
without an entry take the second argument as their outcome. IDs outside the
batch are ignored. An Abort in any entry stops the relay with the whole batch
left claimed, and a `WithFatal` classifier sees the whole `*BatchError`
(`errors.Is` / `errors.As` reach every entry). In ordered mode each failed head
holds only its own key, exactly as with a whole-batch failure.

### Settling messages early

`Publish` returns once per batch, so by default every row of a batch stays
`processing` until the slowest one is done, holding the next row of its
partition, even rows of other partitions already delivered. A publisher that
learns outcomes one by one (a JetStream ack per message) reports each as soon
as it knows it:

```go
func (p *jsPublisher) Publish(ctx context.Context, msgs []outbox.Message) error {
    for _, m := range msgs {
        go func() {
            _, err := p.js.PublishMsg(outbox.MessageContext(ctx, m), toNATS(m))
            _ = outbox.Settle(ctx, m, err) // nil = published; outcomes as in PerMessage
        }()
    }
    // ... wait for all acks
    return nil
}
```

The relay commits early outcomes right away, group-committed within
`WithSettleLinger` (default 1ms) or until `Publish` returns, so a row stops
holding its partition and another worker (`WithConcurrency` > 1) claims the
partition's next row while the batch's slow rows still publish. Ordering per
`PartitionKey` is unchanged. In ordered mode a freed partition wakes an idle
worker at once, not at the next poll: the relay's own workers on the commit,
other instances through the commit's `outbox_messages_done` notification
(LISTEN/NOTIFY enabled). Rows a publisher does not settle take what
`Publish` returns. Leases of the rows still in flight keep being renewed.

- `Settle` outcomes are read like `PerMessage` entries; a fatal one (Abort,
  or one `WithFatal` accepts) leaves that row claimed and stops the relay when
  `Publish` returns, while rows settled before it stay committed.
- Misuse is reported: `ErrNotSettling` outside a relay `Publish` call (or after
  it returned), `ErrUnknownMessage` for a message not in the batch,
  `ErrAlreadySettled` for a second report.
- A publisher that settles every message costs about as many statements as a
  batch return.

### Fatal stop and panics

A fatal error stops the relay and the cleaner, and `Run` returns it to the
caller's lifecycle. The batch it hit is **not** completed, retried or
dead-lettered: its rows stay `processing` under their lease, and another
instance (or the restarted one) re-claims them once the lease expires. Other
workers are cancelled exactly as on a normal shutdown. Fatal errors are:

- `outbox.Abort(err)` returned by the publisher (or by a custom `Executor`);
- a panic anywhere in the relay or cleaner (publisher, hooks, executor, lease
  renewal): it is recovered as `*outbox.PanicError` with the panic `Value` and
  `Stack`, never crashes the process, and is always fatal. A panicked `error`
  value is reachable through `errors.Is` / `errors.As`;
- any error the `WithFatal` classifier accepts. It is consulted on every
  Publish error and on every internal error (claim, renew, completion,
  cleanup) while the context is live; errors caused by cancellation are never
  classified. Default: nothing else is fatal.

```go
outbox.WithFatal(func(err error) bool { return errors.Is(err, errBrokerGone) })
```

The library treats payloads as opaque bytes. Application policy, deadlines,
provider error classification, recipient preferences, and authorization belong
in the caller/publisher. Error text is persisted in `last_error`; set
`outbox.WithErrorRenderer(render)` with the service's renderer
(`xlog.NewErrorRenderer(xlog.RedactURL, xlogpgx.RedactPgError)`) so signed
URLs and database values are stored redacted. Hooks and instrumentation get
the original error.

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
outbox.WithTombstoneRetention(30*24*time.Hour) // keep removed refusals' outcome (default)
outbox.WithCleanupInterval(time.Minute) // optional: 1m by default once any retention is set
```

All data-clearing/deletion policies are **opt-in**. Payload and headers are
cleared atomically with the successful/discarded transition; dead rows retain
them for investigation and replay. Hooks see the cleared fields when enabled.
Other columns, including topic, partition key, and last_error, are not scrubbed.
SQL deletion/clearing does not erase historical WAL, backups, or caller logs.

Dead/discarded retention starts at `finished_at`, published retention at
`published_at`. Cleanup locks selected rows and skips locks, preventing it
from deleting a concurrently requeued job. Pending/processing rows are never
removed by the cleaner. Each dead or discarded row it removes leaves a
tombstone (id, status, finished_at) written in the same statement; the cleaner
removes tombstones after `WithTombstoneRetention` (default 30 days), so all
terminal state stays bounded (see Waiting for delivery).

`WithClearPayloadOnComplete` deliberately leaves dead rows untouched: a dead
row keeps its payload and headers for investigation and `Requeue`, until
`WithDeadRetention` removes it.

`ob.Requeue(ctx, id)` returns `(changed bool, err error)`. Only dead rows qualify;
it resets attempts and terminal/lease state and emits a transactional wake-up.
It uses the **enqueue executor**, so the caller can validate application policy
and requeue in the same transaction. It preserves ID, creation time, and data;
the next attempt receives a new lease token. Replaying a job can repeat an
external side effect. The caller owns permission and eligibility checks.

### Instrumentation

`outbox.WithInstrumentation(i)` plugs telemetry into both ends of the outbox
(default `NoopInstrumentation`). The core has no telemetry dependency; ready
implementations live in `contrib/libs/*` modules:
[`contrib/libs/xtrace`](contrib/libs/xtrace/README.md) traces each delivery as
a new trace linked to the span that enqueued it.

```go
type Instrumentation interface {
    Capture(ctx context.Context, headers map[string]string)
    Deliver(ctx context.Context, m Message) (context.Context, func(Outcome))
}
```

- `Capture` runs at `Enqueue`, in the caller's context, before the insert. What
  it writes into the fresh `headers` map is merged into the message headers
  (stored in the row, sent to the transport); the caller's own entries win and
  the caller's map is never written.
- `Deliver` runs in the relay for every claimed message before `Publish`. The
  context it returns is derived from the publish context (deadline and
  cancellation apply); a Publisher that sends messages one by one gets it with
  `outbox.MessageContext(ctx, m)`. The built-in Valkey publishers do.
- The messages handed to `Publish` also carry `Capture` of their delivery
  context in `Headers` (over the stored entries; the row is not changed), so
  every publisher that maps `Message.Headers` to the transport — NATS headers,
  Kafka headers, Valkey stream fields — carries the delivery on to consumers
  with no context API of its own.
- The returned end func is called exactly once per message with its
  `Outcome`: the committed status (`published`, `pending` for a scheduled
  retry, `dead`, `discarded`, per message under `PerMessage`), or
  `processing` with the cause when the attempt left the row claimed (abort,
  panic, lost lease, failed completion). It runs at that message's own
  commit: a message reported early with `Settle` ends when its outcome
  commits, not when the batch's slowest message does.

### Waiting for delivery

A caller that must not answer before its own messages left the outbox (for
example, a command whose effect other services must see first) asks by the
`Message.ID`s it enqueued:

```go
done, err := ob.Published(ctx, ids...) // one read: true = all delivered
err = ob.WaitPublished(ctx, ids...)    // blocks until delivered, undeliverable, or ctx ends
```

- **Delivered** means published. A dead or discarded row the cleaner removed
  leaves a tombstone (`outbox_tombstones`: id, status, finished_at — no
  payload) for `WithTombstoneRetention` (default 30 days), so its refusal is
  still reported. An id with neither row nor tombstone is delivered when the
  cleaner removes no refused rows (only a published row can be gone), or when
  it is a UUIDv7 created within the horizon — shortest refusal retention +
  tombstone retention − 1h for clock skew — since a refusal that recent would
  still have its tombstone. Any other such id (older, or not a UUIDv7) returns
  an error matching `outbox.ErrOutcomeUnknown` (an `*outbox.UnknownOutcomeError`
  naming the rows), never a false success. Use UUIDv7 ids for exact answers
  with bounded storage, and give the `Outbox` that asks the same retention
  options as the one that cleans. An id never enqueued reads as gone.
- A row that ended `dead` or `discarded` returns an error matching
  `outbox.ErrUndelivered` (an `*outbox.UndeliveredError` naming the rows): it
  will not be delivered unless requeued. A non-uuid id returns `ErrInvalidID`.
- `WaitPublished` returns an error wrapping `ctx.Err()` (and the last failed
  state read, if any) when `ctx` ends first.
- Only the listed rows are read, by primary key, through the relay executor;
  other rows and partitions never hold a wait.

How `WaitPublished` learns the outcome:

1. Every relay (in any process) announces rows reaching a terminal status on
   the `outbox_messages_done` channel, from the statement that completes them,
   so a listener receives it exactly at commit. The payload is
   `<schema>:<status>:<id>,<id>,...`, at most 200 ids per notification.
2. The LISTEN connection of this `Outbox`'s `Run` (the one that also wakes the
   relay) feeds those notifications to the waiting calls: no query per wait
   beyond its first check, made after it registered so no notification is
   missed. Notifications for another schema are ignored.
3. One shared re-read of all waiting ids every `WithWaitRecheck` interval
   (default 1s, at most one query per interval however many calls wait)
   settles what notifications did not: a listener reconnecting, a process that
   does not `Run` this `Outbox` or has no LISTEN pool, or `WithoutNotify`.

So wait on the `Outbox` whose `Run` is active in the same process for
commit-time latency; elsewhere waits still finish, at the recheck pace. A relay
running with `WithoutNotify` announces nothing, so waits on its rows also fall
back to the recheck.

### Upgrade to v1.6

Apply `0004_tombstones.sql` (additive: a new table) before running v1.6: the
cleaner writes tombstones and `Published`/`WaitPublished` read them.

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
Instrumentation/Executor), `config`, `backoff`, `migrations`, `engine`
(store/relay/cleaner/waiters). Concrete publishers live in nested
`contrib/publishers/*` modules and integrations with other libraries in
`contrib/libs/*` (`xtrace`), so Kafka, NATS, Valkey and OpenTelemetry
dependencies never enter the core module's `go.mod`.

## Semantics

- **At-least-once.** A failed batch is retried in full; consumers must
  deduplicate (e.g. on `Message.ID`).
- **Idempotent enqueue.** `Message.ID` is the dedupe key while its row exists:
  enqueuing an existing ID (in the same or another transaction, or twice in
  one batch) is a no-op that keeps the first row and does not abort the
  caller's transaction. Derive the ID from the change to dedupe repeated
  triggers. Once the cleaner deletes a finished row, the same ID can be
  enqueued again, so receivers still deduplicate.
- **Scaling.** Run one relay per instance with a unique `WithInstanceID`.
  Claims use `FOR UPDATE SKIP LOCKED` + a lease (`locked_until`); a crashed
  instance's rows are re-claimed once its lease expires.
- **Retries / dead-letter.** On failure `attempts` increments and the row is
  rescheduled with backoff; after `WithMaxAttempts` it becomes `dead` and stays
  in the table for inspection.
- **Ordering.** Best-effort by `created_at`. `WithOrdered(true)` enforces strict
  per-`PartitionKey` ordering (transaction time, then insertion order, so rows
  written in one transaction keep their order); a `dead` row blocks its
  key until resolved or removed. Published/discarded rows do not block it.
- **Latency.** A trigger fires `NOTIFY`; the relay wakes near-instantly and
  falls back to polling every `WithPollInterval`. Once awake it keeps claiming
  until a pass finds nothing due, so a backlog (including a long run of one
  key in ordered mode) drains without waiting for the next poll.
- **Scheduling.** None: an enqueued row is due immediately; `NextAttemptAt` is
  set only by retry backoff.

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
