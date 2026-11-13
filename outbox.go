// Package outbox implements a transactional outbox for PostgreSQL.
//
// Messages are written to the outbox table inside the caller's business
// transaction; a background relay later claims them with FOR UPDATE SKIP LOCKED
// (leased to an externally supplied instance id) and hands them to a
// transport-agnostic Publisher. Delivery is at-least-once: consumers must
// deduplicate.
//
// This file is the package's single public surface: it re-exports the types and
// constructors from the implementation subpackages (message, port, config,
// backoff, engine, migrations) and provides the Outbox facade. The database
// surface is the Executor interface (Exec, Query, QueryRow); plug your own. The
// logger is the standard library's *slog.Logger, injected via WithLogger.
package outbox

import (
	"context"
	"embed"
	"errors"
	"maps"

	"github.com/gopherex/pg-outbox/backoff"
	"github.com/gopherex/pg-outbox/config"
	"github.com/gopherex/pg-outbox/engine"
	"github.com/gopherex/pg-outbox/message"
	"github.com/gopherex/pg-outbox/migrations"
	"github.com/gopherex/pg-outbox/port"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---- Re-exported types -------------------------------------------------

type (
	// Message is a single outbox row.
	Message = message.Message
	// Status is the lifecycle state of an outbox message.
	Status = message.Status

	// Publisher delivers a batch of claimed messages to a transport.
	Publisher = port.Publisher
	// Codec marshals a value to bytes for storage and back.
	Codec = port.Codec
	// Hooks observes relay activity.
	Hooks           = port.Hooks
	DiscardHooks    = port.DiscardHooks
	PermanentError  = port.PermanentError
	RetryAfterError = port.RetryAfterError
	DiscardError    = port.DiscardError
	AbortError      = port.AbortError
	BatchError      = port.BatchError
	PanicError      = port.PanicError
	// Instrumentation plugs telemetry into Enqueue and delivery.
	Instrumentation     = port.Instrumentation
	Outcome             = port.Outcome
	NoopInstrumentation = port.NoopInstrumentation
	// NoopHooks is the default no-op Hooks implementation.
	NoopHooks = port.NoopHooks
	// Executor is the pluggable pgx query surface (Exec, Query, QueryRow).
	Executor = port.Executor

	// Backoff returns the delay before the retry after a given attempt.
	Backoff = backoff.Backoff

	// Config is the declarative configuration (mapstructure/validate/default).
	Config = config.Config
	// Settings is the resolved runtime configuration.
	Settings = config.Settings
	// Option configures the outbox.
	Option = config.Option
)

// TableName is the fixed outbox table name (only the schema is configurable).
const TableName = message.TableName

// Status values.
const (
	StatusPending    = message.StatusPending
	StatusProcessing = message.StatusProcessing
	StatusPublished  = message.StatusPublished
	StatusDead       = message.StatusDead
	StatusDiscarded  = message.StatusDiscarded
)

// ---- Re-exported options ----------------------------------------------

var (
	WithConfig                 = config.WithConfig
	WithInstanceID             = config.WithInstanceID
	WithSchema                 = config.WithSchema
	WithPollInterval           = config.WithPollInterval
	WithBatchSize              = config.WithBatchSize
	WithLeaseDuration          = config.WithLeaseDuration
	WithConcurrency            = config.WithConcurrency
	WithMaxAttempts            = config.WithMaxAttempts
	WithRetryBackoff           = config.WithRetryBackoff
	WithRetention              = config.WithRetention
	WithDeadRetention          = config.WithDeadRetention
	WithDiscardRetention       = config.WithDiscardRetention
	WithClearPayloadOnComplete = config.WithClearPayloadOnComplete
	WithPublishTimeout         = config.WithPublishTimeout
	WithCleanupInterval        = config.WithCleanupInterval
	WithOrdered                = config.WithOrdered
	WithoutNotify              = config.WithoutNotify
	WithWaitRecheck            = config.WithWaitRecheck
	WithSettleLinger           = config.WithSettleLinger
	WithListenPool             = config.WithListenPool
	WithFatal                  = config.WithFatal
	WithHooks                  = config.WithHooks
	WithInstrumentation        = config.WithInstrumentation
	WithErrorRenderer          = config.WithErrorRenderer
	WithCodec                  = config.WithCodec
	WithLogger                 = config.WithLogger
)

// Publisher outcome controls apply to the whole batch, or per message through
// PerMessage, and preserve wrapped errors.
var (
	Permanent    = port.Permanent
	RetryAfter   = port.RetryAfter
	Discard      = port.Discard
	Abort        = port.Abort
	PerMessage   = port.PerMessage
	ErrLeaseLost = port.ErrLeaseLost
)

// MessageContext returns the context the Instrumentation opened for m's
// delivery, or ctx when there is none. Publishers that send messages one by
// one publish m with it.
var MessageContext = port.MessageContext

// Settle reports one message's outcome from inside Publish as soon as it is
// known, so the row stops holding its partition; see port.Settle.
var Settle = port.Settle

var (
	// ErrNotSettling: Settle outside a relay Publish call or after it returned.
	ErrNotSettling = port.ErrNotSettling
	// ErrUnknownMessage: Settle for a message not in the batch.
	ErrUnknownMessage = port.ErrUnknownMessage
	// ErrAlreadySettled: a second Settle for the same message.
	ErrAlreadySettled = port.ErrAlreadySettled
)

// ---- Re-exported backoff ----------------------------------------------

// ExpBackoff returns an exponential backoff (base doubled per attempt, capped).
var ExpBackoff = backoff.Exp

// DefaultBackoff is exponential 100ms -> 30s with full jitter.
var DefaultBackoff = backoff.Default

// ---- Migrations -------------------------------------------------------

// MigrationsFS returns the embedded migration files for golang-migrate (iofs
// source), goose, atlas, etc. The SQL is schema-unqualified: set search_path on
// the migration connection to install into a non-default schema.
func MigrationsFS() embed.FS { return migrations.FS() }

// Migrations returns the contents of every forward-only *.sql file, ordered by filename,
// ready to pass to Exec for callers applying migrations without a tool.
func Migrations() []string { return migrations.Up() }

// ---- Errors -----------------------------------------------------------

var (
	// ErrEmptyTopic is returned when a message has no topic.
	ErrEmptyTopic = message.ErrEmptyTopic
	// ErrNilPayload is returned when a message payload is nil.
	ErrNilPayload = message.ErrNilPayload
	// ErrInvalidSchema is returned by New when the schema is not a valid identifier.
	ErrInvalidSchema = config.ErrInvalidSchema
	// ErrNoCodec is returned by EnqueueValue when no codec was configured.
	ErrNoCodec = errors.New("outbox: no codec configured (use WithCodec)")
	// ErrNoExecutor is returned by New when no relay Executor was supplied.
	ErrNoExecutor = errors.New("outbox: a relay executor is required")
	// ErrInvalidID is returned by Published / WaitPublished for a non-uuid id.
	ErrInvalidID = engine.ErrInvalidID
	// ErrUndelivered matches an *UndeliveredError through errors.Is.
	ErrUndelivered = engine.ErrUndelivered
	// ErrOutcomeUnknown matches an *UnknownOutcomeError through errors.Is.
	ErrOutcomeUnknown = engine.ErrOutcomeUnknown
)

// UndeliveredError names the rows Published / WaitPublished found dead or
// discarded; they will not be delivered unless requeued.
type UndeliveredError = engine.UndeliveredError

// UnknownOutcomeError names the rows Published / WaitPublished found gone
// while dead or discarded rows are collected too, so their outcome is unknown.
type UnknownOutcomeError = engine.UnknownOutcomeError

// ---- Facade -----------------------------------------------------------

// Outbox is the public facade: enqueue messages (transactionally) and run the
// background relay + cleaner.
type Outbox struct {
	store   engine.Store
	relay   *engine.Relay
	clean   *engine.Cleaner
	waiters *engine.Waiters
	set     config.Settings
}

// New builds an Outbox.
//
//   - exec is the relay/cleaner Executor: it runs the background claim, mark and
//     cleanup queries and must NOT be bound to a business transaction (pass a
//     *pgxpool.Pool, or your own Executor implementation).
//   - enq is the enqueue Executor: pass one that resolves the caller's
//     transaction from the context (e.g. pgtx's tx.DB) so inserts join the
//     business transaction.
//   - pub is the transport publisher.
//
// LISTEN/NOTIFY wake-ups require a *pgxpool.Pool: supply one with WithListenPool,
// or pass a *pgxpool.Pool as exec and it is detected automatically. Without one
// the relay falls back to polling.
func New(exec Executor, enq Executor, pub Publisher, opts ...Option) (*Outbox, error) {
	set := config.Resolve(opts...)
	if !config.ValidSchema(set.Schema) {
		return nil, ErrInvalidSchema
	}
	if exec == nil {
		return nil, ErrNoExecutor
	}

	// Resolve the LISTEN pool: explicit option wins, else detect a *pgxpool.Pool
	// passed as the relay executor.
	listenPool := set.ListenPool
	if listenPool == nil {
		if p, ok := exec.(*pgxpool.Pool); ok {
			listenPool = p
		}
	}

	st := engine.NewStore(exec, enq, listenPool, set.Schema, set.Notify)
	waiters := engine.NewWaiters(set.Schema, st.DeliveryState, set.WaitRecheck, refusalsCollected(set))
	return &Outbox{
		store:   st,
		set:     set,
		waiters: waiters,
		relay:   engine.NewRelay(st, pub, set, waiters),
		clean:   engine.NewCleaner(st, set),
	}, nil
}

// refusalsCollected reports whether the cleaner removes dead or discarded rows.
func refusalsCollected(set config.Settings) bool {
	return set.DeadRetention > 0 || set.DiscardRetention > 0
}

// Published reports whether every listed message is delivered. A row still
// pending or being published makes it false. A row that ended dead or
// discarded returns an error matching ErrUndelivered (an *UndeliveredError
// naming the rows): it will not be delivered unless requeued. ids are the
// Message.IDs given to Enqueue; a non-uuid id returns ErrInvalidID. It reads
// only those rows, by primary key, through the relay executor (never a caller
// transaction), so other rows and partitions do not affect it.
//
// A row that is gone is delivered only while the cleaner removes no refused
// rows (no WithDeadRetention / WithDiscardRetention): then only a published
// row can have been removed. With either retention on, a gone row may have
// been dead or discarded too, and the answer is an error matching
// ErrOutcomeUnknown (an *UnknownOutcomeError naming the rows). Configure the
// Outbox that asks with the same retention as the one that cleans. An id
// never enqueued reads as gone as well.
func (o *Outbox) Published(ctx context.Context, ids ...string) (bool, error) {
	canon, err := engine.CanonicalIDs(ids)
	if err != nil || len(canon) == 0 {
		return err == nil, err
	}
	states, err := o.store.DeliveryState(ctx, canon)
	if err != nil {
		return false, err
	}
	return engine.Verdict(canon, states, refusalsCollected(o.set))
}

// WaitPublished blocks until every listed message is delivered (as Published
// reports it), returning nil; or until one ends dead or discarded, returning
// an error matching ErrUndelivered; or until ctx ends, returning an error that
// wraps ctx.Err() (and the last failed state read, if any).
//
// It learns outcomes from the relays' commit notifications (DoneChannel),
// whichever process published the rows, through the LISTEN connection of this
// Outbox's Run: no query per wait beyond the first check. Without that
// listener (Run not running here, no LISTEN pool, WithoutNotify) or for a
// missed notification, one shared re-read for all waiting calls every
// WithWaitRecheck interval settles it.
func (o *Outbox) WaitPublished(ctx context.Context, ids ...string) error {
	canon, err := engine.CanonicalIDs(ids)
	if err != nil {
		return err
	}
	return o.waiters.Wait(ctx, canon)
}

// Enqueue writes one message inside the caller's current transaction.
func (o *Outbox) Enqueue(ctx context.Context, m Message) error {
	if err := m.Validate(); err != nil {
		return err
	}
	return o.store.Enqueue(ctx, []Message{o.captured(ctx, m)})
}

// EnqueueBatch writes multiple messages inside the caller's current transaction.
func (o *Outbox) EnqueueBatch(ctx context.Context, ms []Message) error {
	out := make([]Message, len(ms))
	for i, m := range ms {
		if err := m.Validate(); err != nil {
			return err
		}
		out[i] = o.captured(ctx, m)
	}
	return o.store.Enqueue(ctx, out)
}

// captured returns m with the instrumentation's headers merged in. The
// caller's headers map is never written, and its entries win.
func (o *Outbox) captured(ctx context.Context, m Message) Message {
	extra := map[string]string{}
	o.set.Instrumentation.Capture(ctx, extra)
	if len(extra) == 0 {
		return m
	}
	headers := make(map[string]string, len(m.Headers)+len(extra))
	maps.Copy(headers, extra)
	maps.Copy(headers, m.Headers)
	m.Headers = headers
	return m
}

// EnqueueValue marshals v with the configured codec and enqueues it. Requires
// WithCodec.
func (o *Outbox) EnqueueValue(ctx context.Context, topic, partitionKey, msgType string, v any) error {
	if o.set.Codec == nil {
		return ErrNoCodec
	}
	payload, ct, err := o.set.Codec.Marshal(v)
	if err != nil {
		return err
	}
	return o.Enqueue(ctx, Message{
		Topic:        topic,
		PartitionKey: partitionKey,
		MessageType:  msgType,
		Payload:      payload,
		ContentType:  ct,
	})
}

// Requeue atomically returns a dead row to pending and resets its attempts.
// It uses the enqueue executor, so authorization and application checks can be
// performed in the same caller transaction. False means no dead row matched.
// IDs and creation time are preserved; the next claim gets a fresh lease token.
func (o *Outbox) Requeue(ctx context.Context, id string) (bool, error) {
	return o.store.Requeue(ctx, id)
}

// Run starts the relay and cleaner and blocks until ctx is cancelled, then
// returns ctx.Err(). A fatal error (an Abort outcome, a recovered panic, or an
// error the WithFatal classifier accepts) stops both and is returned instead;
// the batch it hit stays claimed and is re-claimed after its lease expires.
// The lease owner id was resolved at New time (WithInstanceID, or auto-generated).
func (o *Outbox) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, 2)
	go func() { errs <- o.relay.Run(runCtx) }()
	go func() { errs <- o.clean.Run(runCtx) }()
	var failure error
	for range 2 {
		if err := <-errs; err != nil && failure == nil {
			failure = err
			cancel()
		}
	}
	if failure != nil {
		return failure
	}
	return ctx.Err()
}
