package integration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
	"github.com/jackc/pgx/v5/pgxpool"
)

type publisherFunc func(context.Context, []outbox.Message) error

func (f publisherFunc) Publish(ctx context.Context, ms []outbox.Message) error { return f(ctx, ms) }

type outcomeHooks struct {
	outbox.NoopHooks
	published, discarded, failed, dead atomic.Int32
}

func (h *outcomeHooks) OnPublished(context.Context, []outbox.Message)        { h.published.Add(1) }
func (h *outcomeHooks) OnDiscarded(context.Context, []outbox.Message, error) { h.discarded.Add(1) }
func (h *outcomeHooks) OnFailed(context.Context, outbox.Message, error)      { h.failed.Add(1) }
func (h *outcomeHooks) OnDead(context.Context, outbox.Message)               { h.dead.Add(1) }

type leaseLog struct{ lost chan struct{} }

func (h leaseLog) Enabled(context.Context, slog.Level) bool { return true }
func (h leaseLog) Handle(_ context.Context, r slog.Record) error {
	r.Attrs(func(a slog.Attr) bool {
		if err, ok := a.Value.Any().(error); ok && errors.Is(err, outbox.ErrLeaseLost) {
			select {
			case h.lost <- struct{}{}:
			default:
			}
		}
		return true
	})
	return nil
}
func (h leaseLog) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h leaseLog) WithGroup(string) slog.Handler      { return h }

func relay(t *testing.T, pool *pgxpool.Pool, pub outbox.Publisher, opts ...outbox.Option) (*outbox.Outbox, func()) {
	t.Helper()
	options := append([]outbox.Option{outbox.WithPollInterval(10 * time.Millisecond), outbox.WithBatchSize(1), outbox.WithoutNotify()}, opts...)
	ob, err := outbox.New(pool, pool, pub, options...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = ob.Run(ctx) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("relay did not stop")
		}
	}
	t.Cleanup(stop)
	return ob, stop
}

func enqueue(t *testing.T, ob *outbox.Outbox, topic string) {
	t.Helper()
	if err := ob.Enqueue(context.Background(), outbox.Message{Topic: topic, Payload: []byte("private payload"), Headers: map[string]string{"private": "header"}}); err != nil {
		t.Fatal(err)
	}
}

func waitStatus(t *testing.T, pool *pgxpool.Pool, topic, want string) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool {
		var got string
		err := pool.QueryRow(context.Background(), "SELECT status FROM outbox_messages WHERE topic=$1", topic).Scan(&got)
		return err == nil && got == want
	})
}

func TestDeliveryControls(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	t.Run("permanent skips retries and retains payload", func(t *testing.T) {
		var calls atomic.Int32
		ob, _ := relay(t, pool, publisherFunc(func(context.Context, []outbox.Message) error {
			calls.Add(1)
			return fmt.Errorf("wrapped: %w", outbox.Permanent(errors.New("invalid destination")))
		}), outbox.WithClearPayloadOnComplete(true))
		enqueue(t, ob, "permanent")
		waitStatus(t, pool, "permanent", "dead")
		var attempts int
		var payload, reason string
		var finished bool
		err := pool.QueryRow(ctx, "SELECT attempts, convert_from(payload,'UTF8'), last_error, finished_at IS NOT NULL FROM outbox_messages WHERE topic='permanent'").Scan(&attempts, &payload, &reason, &finished)
		if err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 1 || attempts != 1 || payload != "private payload" || reason != "wrapped: invalid destination" || !finished {
			t.Fatalf("bad terminal state: calls=%d attempts=%d payload=%q reason=%q finished=%v", calls.Load(), attempts, payload, reason, finished)
		}
	})

	t.Run("retry delay is persisted and honors backoff", func(t *testing.T) {
		for i, requested := range []time.Duration{time.Second, 5 * time.Second} {
			topic := fmt.Sprintf("delay%d", i)
			ob, stop := relay(t, pool, publisherFunc(func(context.Context, []outbox.Message) error {
				return fmt.Errorf("wrapped: %w", outbox.RetryAfter(errors.New("busy"), requested))
			}), outbox.WithRetryBackoff(func(int) time.Duration { return 3 * time.Second }))
			enqueue(t, ob, topic)
			waitFor(t, 5*time.Second, func() bool {
				var ok bool
				err := pool.QueryRow(ctx, "SELECT status='pending' AND attempts=1 AND next_attempt_at > now()+interval '2 seconds' FROM outbox_messages WHERE topic=$1", topic).Scan(&ok)
				return err == nil && ok
			})
			stop()
			var seconds float64
			if err := pool.QueryRow(ctx, "SELECT extract(epoch FROM next_attempt_at-created_at)::float8 FROM outbox_messages WHERE topic=$1", topic).Scan(&seconds); err != nil {
				t.Fatal(err)
			}
			minimum := 3.0
			if requested == 5*time.Second {
				minimum = 5
			}
			if seconds < minimum {
				t.Fatalf("delay %f below %f", seconds, minimum)
			}
			// Keep this pending row out of subsequent subtests' relays.
			if _, err := pool.Exec(ctx, "DELETE FROM outbox_messages WHERE topic=$1", topic); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("success and discard scrub atomically", func(t *testing.T) {
		for _, discard := range []bool{false, true} {
			topic := fmt.Sprintf("complete%v", discard)
			hooks := &outcomeHooks{}
			ob, stop := relay(t, pool, publisherFunc(func(context.Context, []outbox.Message) error {
				if discard {
					return outbox.Discard(errors.New("caller policy"))
				}
				return nil
			}), outbox.WithClearPayloadOnComplete(true), outbox.WithHooks(hooks))
			enqueue(t, ob, topic)
			want := "published"
			if discard {
				want = "discarded"
			}
			waitStatus(t, pool, topic, want)
			waitFor(t, time.Second, func() bool { return hooks.published.Load()+hooks.discarded.Load() == 1 })
			var empty, finished bool
			if err := pool.QueryRow(ctx, "SELECT octet_length(payload)=0 AND headers='{}'::jsonb, finished_at IS NOT NULL FROM outbox_messages WHERE topic=$1", topic).Scan(&empty, &finished); err != nil {
				t.Fatal(err)
			}
			if !empty || !finished || hooks.dead.Load() != 0 || hooks.failed.Load() != 0 {
				t.Fatal("wrong completion")
			}
			if discard && hooks.published.Load() != 0 {
				t.Fatal("discard reported as published")
			}
			stop()
		}
	})

	t.Run("timeout is an ordinary failed attempt", func(t *testing.T) {
		ob, _ := relay(t, pool, publisherFunc(func(ctx context.Context, _ []outbox.Message) error { <-ctx.Done(); return ctx.Err() }), outbox.WithPublishTimeout(50*time.Millisecond), outbox.WithMaxAttempts(1))
		enqueue(t, ob, "timeout")
		waitStatus(t, pool, "timeout", "dead")
	})

	t.Run("lease renewal prevents concurrent reclaim", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		ob, _ := relay(t, pool, publisherFunc(func(ctx context.Context, _ []outbox.Message) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}), outbox.WithLeaseDuration(600*time.Millisecond))
		enqueue(t, ob, "renew")
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("no publish")
		}
		var rivalCalls atomic.Int32
		_, stop := relay(t, pool, publisherFunc(func(context.Context, []outbox.Message) error { rivalCalls.Add(1); return nil }), outbox.WithLeaseDuration(600*time.Millisecond))
		timer := time.NewTimer(1500 * time.Millisecond)
		defer timer.Stop()
		<-timer.C
		close(release)
		waitStatus(t, pool, "renew", "published")
		stop()
		if rivalCalls.Load() != 0 {
			t.Fatalf("renewed message reclaimed %d times", rivalCalls.Load())
		}
	})

	t.Run("stale completions never overwrite a new attempt", func(t *testing.T) {
		for _, outcome := range []string{"success", "retry", "dead", "discard"} {
			t.Run(outcome, func(t *testing.T) {
				entered := make(chan struct{})
				release := make(chan struct{})
				hooks := &outcomeHooks{}
				lost := make(chan struct{}, 1)
				ob, _ := relay(t, pool, publisherFunc(func(ctx context.Context, _ []outbox.Message) error {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					switch outcome {
					case "retry":
						return errors.New("temporary")
					case "dead":
						return outbox.Permanent(errors.New("permanent"))
					case "discard":
						return outbox.Discard(nil)
					}
					return nil
				}), outbox.WithInstanceID("same-process"), outbox.WithLeaseDuration(30*time.Second), outbox.WithHooks(hooks), outbox.WithLogger(slog.New(leaseLog{lost})))
				topic := "stale-" + outcome
				enqueue(t, ob, topic)
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("no publish")
				}
				// Same process id, different claim token: this also models replay after attempts reset.
				if _, err := pool.Exec(ctx, "UPDATE outbox_messages SET lease_token=gen_random_uuid(), attempts=2, locked_until=now()+interval '1 hour' WHERE topic=$1", topic); err != nil {
					t.Fatal(err)
				}
				close(release)
				select {
				case <-lost:
				case <-time.After(5 * time.Second):
					t.Fatal("stale completion was not fenced")
				}
				var intact bool
				if err := pool.QueryRow(ctx, "SELECT status='processing' AND attempts=2 AND locked_by='same-process' AND finished_at IS NULL FROM outbox_messages WHERE topic=$1", topic).Scan(&intact); err != nil {
					t.Fatal(err)
				}
				if !intact || hooks.published.Load()+hooks.discarded.Load()+hooks.dead.Load()+hooks.failed.Load() != 0 {
					t.Fatal("stale attempt changed state or emitted outcome")
				}
			})
		}
	})

	t.Run("loss of lease cancels publisher", func(t *testing.T) {
		entered := make(chan struct{})
		cancelled := make(chan struct{})
		ob, _ := relay(t, pool, publisherFunc(func(ctx context.Context, _ []outbox.Message) error {
			close(entered)
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		}), outbox.WithLeaseDuration(600*time.Millisecond))
		enqueue(t, ob, "lost")
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("no publish")
		}
		if _, err := pool.Exec(ctx, "UPDATE outbox_messages SET lease_token=gen_random_uuid(), locked_until=now()+interval '1 hour' WHERE topic='lost'"); err != nil {
			t.Fatal(err)
		}
		select {
		case <-cancelled:
		case <-time.After(5 * time.Second):
			t.Fatal("publisher was not cancelled")
		}
		waitStatus(t, pool, "lost", "processing")
	})

	t.Run("requeue joins caller transaction", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		ob, err := outbox.New(pool, tx, nil)
		if err != nil {
			t.Fatal(err)
		}
		var id string
		if err := pool.QueryRow(ctx, "SELECT id FROM outbox_messages WHERE topic='permanent'").Scan(&id); err != nil {
			t.Fatal(err)
		}
		ok, err := ob.Requeue(ctx, id)
		if err != nil || !ok {
			t.Fatalf("requeue: %v %v", ok, err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		waitStatus(t, pool, "permanent", "dead")
		ob, err = outbox.New(pool, pool, nil)
		if err != nil {
			t.Fatal(err)
		}
		ok, err = ob.Requeue(ctx, id)
		if err != nil || !ok {
			t.Fatalf("requeue: %v %v", ok, err)
		}
		var reset bool
		if err := pool.QueryRow(ctx, "SELECT status='pending' AND attempts=0 AND finished_at IS NULL AND last_error IS NULL AND lease_token IS NULL FROM outbox_messages WHERE id=$1", id).Scan(&reset); err != nil {
			t.Fatal(err)
		}
		if !reset {
			t.Fatal("requeue did not reset attempt state")
		}
		ok, err = ob.Requeue(ctx, id)
		if err != nil || ok {
			t.Fatalf("non-dead requeue: %v %v", ok, err)
		}
		if _, err := pool.Exec(ctx, "DELETE FROM outbox_messages WHERE id=$1", id); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("terminal retention is independent and opt-in", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `INSERT INTO outbox_messages(topic,payload,status,finished_at,published_at) VALUES
   ('clean-dead','x','dead',now()-interval '1 hour',NULL),
   ('clean-discard','x','discarded',now()-interval '1 hour',NULL),
   ('keep-published','x','published',now()-interval '1 hour',now()-interval '1 hour')`); err != nil {
			t.Fatal(err)
		}
		_, _ = relay(t, pool, publisherFunc(func(context.Context, []outbox.Message) error { return nil }), outbox.WithDeadRetention(time.Minute), outbox.WithDiscardRetention(time.Minute), outbox.WithCleanupInterval(10*time.Millisecond))
		waitFor(t, 5*time.Second, func() bool {
			var n int
			err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_messages WHERE topic IN ('clean-dead','clean-discard')").Scan(&n)
			return err == nil && n == 0
		})
		waitStatus(t, pool, "keep-published", "published")
		waitStatus(t, pool, "timeout", "dead") // recently terminal, despite older creation timestamps being irrelevant
	})
}

func TestMigrationUpgrade(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA upgrade; SET search_path TO upgrade"); err != nil {
		t.Fatal(err)
	}
	migrations := outbox.Migrations()
	if _, err := conn.Exec(ctx, migrations[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO outbox_messages(topic,payload,status,attempts,locked_by,locked_until,created_at)
 VALUES ('old-dead','secret','dead',3,NULL,NULL,now()-interval '1 year'),
 ('old-processing','secret','processing',1,'old-worker',now()-interval '1 minute',now())`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // migration is also safe for callers replaying Migrations() on startup
		if _, err := conn.Exec(ctx, migrations[1]); err != nil {
			t.Fatal(err)
		}
	}
	var kept bool
	if err := conn.QueryRow(ctx, "SELECT payload='secret'::bytea AND attempts=3 AND finished_at > now()-interval '1 minute' FROM outbox_messages WHERE topic='old-dead'").Scan(&kept); err != nil {
		t.Fatal(err)
	}
	if !kept {
		t.Fatal("upgrade lost legacy data or used creation time for dead retention")
	}
	// Restore pooled connection state before returning it to the pool.
	if _, err := conn.Exec(ctx, "SET search_path TO public"); err != nil {
		t.Fatal(err)
	}
	ob, _ := relay(t, pool, publisherFunc(func(_ context.Context, ms []outbox.Message) error {
		if ms[0].LeaseToken == "" {
			return errors.New("no claim token")
		}
		return nil
	}), outbox.WithSchema("upgrade"))
	_ = ob
	waitFor(t, 5*time.Second, func() bool {
		var done bool
		err := pool.QueryRow(ctx, "SELECT status='published' AND attempts=2 FROM upgrade.outbox_messages WHERE topic='old-processing'").Scan(&done)
		return err == nil && done
	})
}

func TestEnqueueAtomicWithCallerTransaction(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "CREATE TABLE business (id integer PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for _, commit := range []bool{false, true} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ob, err := outbox.New(pool, tx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO business VALUES (1)"); err != nil {
			t.Fatal(err)
		}
		if err := ob.EnqueueBatch(ctx, []outbox.Message{{Topic: "one", Payload: []byte("a")}, {Topic: "two", Payload: []byte("b")}}); err != nil {
			t.Fatal(err)
		}
		if commit {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
		var business, jobs int
		if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM business),(SELECT count(*) FROM outbox_messages)").Scan(&business, &jobs); err != nil {
			t.Fatal(err)
		}
		if commit && (business != 1 || jobs != 2) || !commit && (business != 0 || jobs != 0) {
			t.Fatalf("commit=%v business=%d jobs=%d", commit, business, jobs)
		}
	}
}

func TestOrderedDiscardAndReplay(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	// Equal creation timestamps are normal for a transactional EnqueueBatch.
	if _, err := pool.Exec(ctx, `INSERT INTO outbox_messages(id,topic,partition_key,payload,created_at) VALUES
 ('00000000-0000-0000-0000-000000000001','ordered','key','one',now()),
 ('00000000-0000-0000-0000-000000000002','ordered','key','two',now()),
 ('00000000-0000-0000-0000-000000000003','ordered','key','three',now())`); err != nil {
		t.Fatal(err)
	}
	var replay atomic.Bool
	var third atomic.Int32
	ob, _ := relay(t, pool, publisherFunc(func(_ context.Context, ms []outbox.Message) error {
		if len(ms) != 1 {
			return errors.New("claimed multiple rows in one ordered partition")
		}
		switch string(ms[0].Payload) {
		case "one":
			return outbox.Discard(nil)
		case "two":
			if !replay.Load() {
				return outbox.Permanent(errors.New("blocked"))
			}
		case "three":
			third.Add(1)
		}
		return nil
	}), outbox.WithOrdered(true), outbox.WithBatchSize(10), outbox.WithConcurrency(2))
	waitFor(t, 5*time.Second, func() bool {
		var count int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_messages WHERE status='dead'").Scan(&count)
		return err == nil && count == 1
	})
	if third.Load() != 0 {
		t.Fatal("dead predecessor did not block partition")
	}
	replay.Store(true)
	if ok, err := ob.Requeue(ctx, "00000000-0000-0000-0000-000000000002"); err != nil || !ok {
		t.Fatalf("requeue: %v %v", ok, err)
	}
	waitFor(t, 5*time.Second, func() bool {
		var count int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_messages WHERE status='published'").Scan(&count)
		return err == nil && count == 2
	})
	if third.Load() != 1 {
		t.Fatalf("third delivered %d times", third.Load())
	}
}

func TestExpiredLeaseCannotBeRenewed(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	lost := make(chan struct{}, 1)
	ob, _ := relay(t, pool, publisherFunc(func(ctx context.Context, _ []outbox.Message) error {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		return nil
	}),
		outbox.WithLeaseDuration(600*time.Millisecond), outbox.WithLogger(slog.New(leaseLog{lost})))
	enqueue(t, ob, "expired-lease")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no publish")
	}
	// Prevent reclaim so this tests expiry without a replacement token.
	if _, err := pool.Exec(ctx, "UPDATE outbox_messages SET locked_until=now()-interval '1 second', next_attempt_at=now()+interval '1 hour' WHERE topic='expired-lease'"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("expired lease was renewed")
	}
	select {
	case <-lost:
	case <-time.After(5 * time.Second):
		t.Fatal("missing lease failure")
	}
	var expired bool
	if err := pool.QueryRow(ctx, "SELECT status='processing' AND locked_until < now() FROM outbox_messages WHERE topic='expired-lease'").Scan(&expired); err != nil {
		t.Fatal(err)
	}
	if !expired {
		t.Fatal("expired lease was revived or completed")
	}
}

func TestCleanupSkipsTransactionalReplay(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO outbox_messages(id,topic,payload,status,finished_at) VALUES
 ('00000000-0000-0000-0000-000000000001','replay-lock','x','dead',now()-interval '1 hour'),
 ('00000000-0000-0000-0000-000000000002','cleanup-witness','x','dead',now()-interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	queued, err := outbox.New(pool, tx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := queued.Requeue(ctx, "00000000-0000-0000-0000-000000000001"); err != nil || !ok {
		t.Fatalf("requeue: %v %v", ok, err)
	}
	_, stop := relay(t, pool, publisherFunc(func(context.Context, []outbox.Message) error { return nil }), outbox.WithDeadRetention(time.Minute), outbox.WithCleanupInterval(10*time.Millisecond))
	waitFor(t, 5*time.Second, func() bool {
		var n int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_messages WHERE topic='cleanup-witness'").Scan(&n)
		return err == nil && n == 0
	})
	stop()
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, pool, "replay-lock", "pending")
}
