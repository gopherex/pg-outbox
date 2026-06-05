// Package port defines the pluggable integration interfaces of the outbox:
// the transport Publisher, the value Codec, observability Hooks, and the
// database Executor.
package port

import (
	"context"

	"github.com/gopherex/pg-outbox/message"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Publisher delivers a batch of claimed messages to a transport. It is the only
// integration point a transport adapter must implement.
//
// A non-nil error fails the whole batch: every message is retried (or
// dead-lettered) per the relay's policy. Because delivery is at-least-once and a
// partial-success batch is re-published in full, consumers must deduplicate. For
// strict per-message isolation, run the relay with batch size 1.
type Publisher interface {
	Publish(ctx context.Context, msgs []message.Message) error
}

// Codec marshals a value to bytes for storage and back. Concrete codecs (JSON,
// protobuf, …) ship separately; the core only defines the interface.
type Codec interface {
	Marshal(v any) (payload []byte, contentType string, err error)
	Unmarshal(payload []byte, v any) error
}

// Hooks observes relay activity. Implement it to wire metrics/tracing. All
// methods must be non-blocking and must not panic.
type Hooks interface {
	OnPublished(ctx context.Context, msgs []message.Message)
	OnFailed(ctx context.Context, msg message.Message, err error)
	OnDead(ctx context.Context, msg message.Message)
	OnCleanup(ctx context.Context, deleted int)
}

// NoopHooks is the default Hooks implementation; every method is a no-op.
type NoopHooks struct{}

func (NoopHooks) OnPublished(context.Context, []message.Message)   {}
func (NoopHooks) OnFailed(context.Context, message.Message, error) {}
func (NoopHooks) OnDead(context.Context, message.Message)          {}
func (NoopHooks) OnCleanup(context.Context, int)                   {}

// Executor is the pgx query surface the outbox needs. It is intentionally the
// common subset of *pgxpool.Pool, pgx.Tx, *pgx.Conn and pgtx's tx.DB — so any
// of them satisfies it, and you can plug your own implementation to control
// pooling, statement routing, retries or instrumentation.
//
// Two executors are supplied to the outbox:
//
//   - the relay executor runs the background claim/mark/cleanup queries; it must
//     NOT be bound to a business transaction.
//   - the enqueue executor runs the transactional INSERT; pass one that resolves
//     the caller's transaction from the context (e.g. pgtx's tx.DB) so messages
//     are written atomically with business data.
type Executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
