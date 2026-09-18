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
	`next_attempt_at, last_error, created_at, published_at, lease_token, finished_at`

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
          AND (e.created_at, e.id) < (o.created_at, o.id)
          AND e.status NOT IN ('published', 'discarded'))`, s.table)
	}
	q := fmt.Sprintf(`
UPDATE %s SET
    status = 'processing',
    locked_by = $1,
    locked_until = clock_timestamp() + make_interval(secs => $2),
    lease_token = gen_random_uuid(),
    attempts = attempts + 1
WHERE id IN (
    SELECT o.id FROM %s o
    WHERE o.next_attempt_at <= now()
      AND (o.status = 'pending' OR (o.status = 'processing' AND o.locked_until < now()))%s
    ORDER BY o.created_at, o.id
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

// claimKeys identifies attempts rather than processes. Reusing an InstanceID
// (or resetting attempts on replay) must never let an old worker finish new work.
func claimKeys(msgs []message.Message) ([]string, []string) {
	ids, tokens := make([]string, len(msgs)), make([]string, len(msgs))
	for i, m := range msgs {
		ids[i], tokens[i] = m.ID, m.LeaseToken
	}
	return ids, tokens
}

// renew extends only live leases owned by these attempts. Never resurrect an
// expired lease, even if another worker has not claimed it yet.
func (s Store) renew(ctx context.Context, msgs []message.Message, duration time.Duration) error {
	ids, tokens := claimKeys(msgs)
	tag, err := s.exec.Exec(ctx, fmt.Sprintf(`UPDATE %s SET
 locked_until=clock_timestamp()+make_interval(secs => $3)
 WHERE (id, lease_token) IN (SELECT * FROM unnest($1::uuid[], $2::uuid[]))
 AND status='processing' AND locked_until > clock_timestamp()`, s.table), ids, tokens, duration.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(msgs)) {
		return port.ErrLeaseLost
	}
	return nil
}

// finish changes only still-owned attempts. Returned rows are the committed
// outcomes; hooks must never report success for a stale completion.
func (s Store) finish(ctx context.Context, msgs []message.Message, status message.Status, reason string, delay time.Duration, clear bool) ([]message.Message, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	ids, tokens := claimKeys(msgs)
	rows, err := s.exec.Query(ctx, fmt.Sprintf(`UPDATE %s SET
 status=$3,
 next_attempt_at=CASE WHEN $3='pending' THEN clock_timestamp()+make_interval(secs => $5) ELSE next_attempt_at END,
 last_error=$4,
 published_at=CASE WHEN $3='published' THEN clock_timestamp() ELSE NULL END,
 finished_at=CASE WHEN $3='pending' THEN NULL ELSE clock_timestamp() END,
 payload=CASE WHEN $6 AND $3 IN ('published','discarded') THEN ''::bytea ELSE payload END,
 headers=CASE WHEN $6 AND $3 IN ('published','discarded') THEN '{}'::jsonb ELSE headers END,
 locked_by=NULL, locked_until=NULL, lease_token=NULL
 WHERE (id, lease_token) IN (SELECT * FROM unnest($1::uuid[], $2::uuid[]))
 AND status='processing' AND locked_until > clock_timestamp()
 RETURNING %s`, s.table, messageColumns), ids, tokens, string(status), nullStr(reason), delay.Seconds(), clear)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

// Requeue uses the caller's transactional executor. A locked row cannot be
// removed by cleanup concurrently. Notifications become visible on commit.
func (s Store) Requeue(ctx context.Context, id string) (bool, error) {
	tag, err := s.enq.Exec(ctx, fmt.Sprintf(`WITH changed AS (
 UPDATE %s SET status='pending', attempts=0, next_attempt_at=clock_timestamp(),
 last_error=NULL, finished_at=NULL, published_at=NULL,
 locked_by=NULL, locked_until=NULL, lease_token=NULL
 WHERE id=$1 AND status='dead' RETURNING id
 ) SELECT pg_notify('%s','') FROM changed`, s.table, NotifyChannel), id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// cleanup locks candidates so a concurrent replay cannot turn one into a
// pending row between selection and deletion. Each retention is opt-in.
func (s Store) cleanup(ctx context.Context, set config.Settings) (int64, error) {
	tag, err := s.exec.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id IN (
 SELECT id FROM %s WHERE
 ($1::double precision > 0 AND status='published' AND published_at < now()-make_interval(secs => $1)) OR
 ($2::double precision > 0 AND status='dead' AND finished_at < now()-make_interval(secs => $2)) OR
 ($3::double precision > 0 AND status='discarded' AND finished_at < now()-make_interval(secs => $3))
 ORDER BY COALESCE(finished_at, published_at), id
 LIMIT $4 FOR UPDATE SKIP LOCKED
 )`, s.table, s.table), set.Retention.Seconds(), set.DeadRetention.Seconds(), set.DiscardRetention.Seconds(), set.BatchSize)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func scanMessages(rows pgx.Rows) ([]message.Message, error) {
	var out []message.Message
	for rows.Next() {
		var (
			m                 message.Message
			pk, ct, mt        *string
			lb, le, token     *string
			statusStr         string
			ma                *int
			lu, pub, finished *time.Time
			headers           []byte
		)
		if err := rows.Scan(&m.ID, &m.Topic, &pk, &m.Payload, &headers, &ct, &mt,
			&statusStr, &m.Attempts, &ma, &lb, &lu, &m.NextAttemptAt, &le,
			&m.CreatedAt, &pub, &token, &finished); err != nil {
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
		m.LeaseToken = derefStr(token)
		m.FinishedAt = finished
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
