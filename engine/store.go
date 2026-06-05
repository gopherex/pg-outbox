// Package engine implements the outbox background machinery: the store
// (enqueue/claim/mark/cleanup queries) and the relay and cleaner workers.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gopherex/pg-outbox/config"
	"github.com/gopherex/pg-outbox/message"
	"github.com/gopherex/pg-outbox/port"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NotifyChannel is the fixed LISTEN/NOTIFY channel (matches the migration
// trigger). It cannot be parameterized; cross-schema wake-ups are harmless
// because SKIP LOCKED deduplicates claims.
const NotifyChannel = "outbox_messages"

const messageColumns = `id, topic, partition_key, payload, headers, content_type, ` +
	`message_type, status, attempts, max_attempts, locked_by, locked_until, ` +
	`next_attempt_at, last_error, created_at, published_at`

// Store runs the outbox SQL. The relay/cleaner queries use exec (must not be
// tx-bound); enqueue uses enq (joins the caller's business tx). pool is optional
// and only LISTEN/NOTIFY needs it.
type Store struct {
	exec  port.Executor
	enq   port.Executor
	pool  *pgxpool.Pool
	table string
}

// NewStore builds a Store. schema is assumed already validated.
func NewStore(exec, enq port.Executor, pool *pgxpool.Pool, schema string) Store {
	return Store{exec: exec, enq: enq, pool: pool, table: fmt.Sprintf("%q.%s", schema, message.TableName)}
}

// Table returns the schema-qualified table identifier.
func (s Store) Table() string { return s.table }

// Enqueue inserts messages using the transactional Executor, so the rows land in
// the caller's business transaction.
func (s Store) Enqueue(ctx context.Context, msgs []message.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	const cols = 8
	var b strings.Builder
	fmt.Fprintf(&b, `INSERT INTO %s `+
		`(id, topic, partition_key, payload, headers, content_type, message_type, max_attempts) VALUES `, s.table)
	args := make([]any, 0, len(msgs)*cols)
	for i, m := range msgs {
		if i > 0 {
			b.WriteByte(',')
		}
		n := i * cols
		fmt.Fprintf(&b, "(COALESCE($%d::uuid, gen_random_uuid()),$%d,$%d,$%d,$%d::jsonb,$%d,$%d,$%d)",
			n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8)

		headers := m.Headers
		if headers == nil {
			headers = map[string]string{}
		}
		hb, err := json.Marshal(headers)
		if err != nil {
			return fmt.Errorf("outbox: marshal headers: %w", err)
		}
		args = append(args,
			nullStr(m.ID), m.Topic, nullStr(m.PartitionKey), m.Payload, string(hb),
			nullStr(m.ContentType), nullStr(m.MessageType), nullInt(m.MaxAttempts))
	}
	_, err := s.enq.Exec(ctx, b.String(), args...)
	return err
}

// claim atomically leases up to set.BatchSize ready rows to set.InstanceID.
func (s Store) claim(ctx context.Context, set config.Settings) ([]message.Message, error) {
	ordered := ""
	if set.Ordered {
		ordered = fmt.Sprintf(`
    AND NOT EXISTS (
        SELECT 1 FROM %s e
        WHERE o.partition_key IS NOT NULL
          AND e.partition_key = o.partition_key
          AND e.created_at < o.created_at
          AND e.status <> 'published')`, s.table)
	}
	q := fmt.Sprintf(`
UPDATE %s SET
    status = 'processing',
    locked_by = $1,
    locked_until = now() + make_interval(secs => $2),
    attempts = attempts + 1
WHERE id IN (
    SELECT o.id FROM %s o
    WHERE o.next_attempt_at <= now()
      AND (o.status = 'pending' OR (o.status = 'processing' AND o.locked_until < now()))%s
    ORDER BY o.created_at
    FOR UPDATE SKIP LOCKED
    LIMIT $3
)
RETURNING %s`, s.table, s.table, ordered, messageColumns)

	rows, err := s.exec.Query(ctx, q, set.InstanceID, set.LeaseDuration.Seconds(), set.BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

// markPublished marks the given ids published.
func (s Store) markPublished(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.exec.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET status='published', published_at=now(), locked_by=NULL, locked_until=NULL `+
			`WHERE id = ANY($1)`, s.table), ids)
	return err
}

// markRetry reschedules a message after a transient failure.
func (s Store) markRetry(ctx context.Context, id, errMsg string, delay time.Duration) error {
	_, err := s.exec.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET status='pending', next_attempt_at=now()+make_interval(secs => $2), `+
			`last_error=$3, locked_by=NULL, locked_until=NULL WHERE id=$1`, s.table),
		id, delay.Seconds(), errMsg)
	return err
}

// markDead moves a message to the dead state after exhausting attempts.
func (s Store) markDead(ctx context.Context, id, errMsg string) error {
	_, err := s.exec.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET status='dead', last_error=$2, locked_by=NULL, locked_until=NULL `+
			`WHERE id=$1`, s.table), id, errMsg)
	return err
}

// cleanup deletes up to limit published rows older than ttl. Returns the number
// deleted.
func (s Store) cleanup(ctx context.Context, ttl time.Duration, limit int) (int64, error) {
	tag, err := s.exec.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE id IN (
            SELECT id FROM %s
            WHERE status='published' AND published_at < now()-make_interval(secs => $1)
            LIMIT $2)`, s.table, s.table), ttl.Seconds(), limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func scanMessages(rows pgx.Rows) ([]message.Message, error) {
	var out []message.Message
	for rows.Next() {
		var (
			m          message.Message
			pk, ct, mt *string
			lb, le     *string
			statusStr  string
			ma         *int
			lu, pub    *time.Time
			headers    []byte
		)
		if err := rows.Scan(&m.ID, &m.Topic, &pk, &m.Payload, &headers, &ct, &mt,
			&statusStr, &m.Attempts, &ma, &lb, &lu, &m.NextAttemptAt, &le,
			&m.CreatedAt, &pub); err != nil {
			return nil, err
		}
		m.Status = message.Status(statusStr)
		m.PartitionKey = derefStr(pk)
		m.ContentType = derefStr(ct)
		m.MessageType = derefStr(mt)
		m.LockedBy = derefStr(lb)
		m.LastError = derefStr(le)
		m.MaxAttempts = ma
		m.LockedUntil = lu
		m.PublishedAt = pub
		if len(headers) > 0 {
			if err := json.Unmarshal(headers, &m.Headers); err != nil {
				return nil, fmt.Errorf("outbox: unmarshal headers: %w", err)
			}
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullInt(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
