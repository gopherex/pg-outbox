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
	"sync"

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
	Hooks = port.Hooks
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
)

// ---- Re-exported options ----------------------------------------------

var (
	WithConfig          = config.WithConfig
	WithInstanceID      = config.WithInstanceID
	WithSchema          = config.WithSchema
	WithPollInterval    = config.WithPollInterval
	WithBatchSize       = config.WithBatchSize
	WithLeaseDuration   = config.WithLeaseDuration
	WithConcurrency     = config.WithConcurrency
	WithMaxAttempts     = config.WithMaxAttempts
	WithRetryBackoff    = config.WithRetryBackoff
	WithRetention       = config.WithRetention
	WithCleanupInterval = config.WithCleanupInterval
	WithOrdered         = config.WithOrdered
	WithoutNotify       = config.WithoutNotify
	WithListenPool      = config.WithListenPool
	WithHooks           = config.WithHooks
	WithCodec           = config.WithCodec
	WithLogger          = config.WithLogger
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

// Migrations returns the contents of every *.up.sql file, ordered by filename,
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
)

// ---- Facade -----------------------------------------------------------

// Outbox is the public facade: enqueue messages (transactionally) and run the
// background relay + cleaner.
type Outbox struct {
	store engine.Store
	relay *engine.Relay
	clean *engine.Cleaner
	set   config.Settings
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

	st := engine.NewStore(exec, enq, listenPool, set.Schema)
	return &Outbox{
		store: st,
		set:   set,
		relay: engine.NewRelay(st, pub, set),
		clean: engine.NewCleaner(st, set),
	}, nil
}

// Enqueue writes one message inside the caller's current transaction.
func (o *Outbox) Enqueue(ctx context.Context, m Message) error {
	if err := m.Validate(); err != nil {
		return err
	}
	return o.store.Enqueue(ctx, []Message{m})
}

// EnqueueBatch writes multiple messages inside the caller's current transaction.
func (o *Outbox) EnqueueBatch(ctx context.Context, ms []Message) error {
	for _, m := range ms {
		if err := m.Validate(); err != nil {
			return err
		}
	}
	return o.store.Enqueue(ctx, ms)
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

// Run starts the relay and cleaner and blocks until ctx is cancelled. The lease
// owner id was resolved at New time (WithInstanceID, or auto-generated).
func (o *Outbox) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := o.relay.Run(ctx); err != nil {
			o.set.Logger.Error("outbox: relay stopped", "err", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := o.clean.Run(ctx); err != nil {
			o.set.Logger.Error("outbox: cleaner stopped", "err", err)
		}
	}()
	wg.Wait()
	return ctx.Err()
}
