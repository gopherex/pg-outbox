package integration

import (
	"context"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
	"github.com/jackc/pgx/v5/pgxpool"
)

// End-to-end: a message enqueued is delivered by the running relay.
func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newTestPool(t)
	pub := &capturePublisher{}
	ob, err := outbox.New(pool, pool, pub,
		outbox.WithInstanceID("e2e"),
		outbox.WithPollInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ob.Enqueue(ctx, outbox.Message{Topic: "orders", Payload: []byte("hello")}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	done := make(chan struct{})
	go func() { _ = ob.Run(ctx); close(done) }()

	waitFor(t, 5*time.Second, func() bool { return len(pub.published()) == 1 })
	if got := pub.published()[0]; got.Topic != "orders" || string(got.Payload) != "hello" {
		t.Fatalf("unexpected message: %+v", got)
	}
	cancel()
	<-done
}

// Two relays with distinct instance ids share a DB; every enqueued message is
// delivered exactly once across them (SKIP LOCKED + lease).
func TestNoDoubleDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newTestPool(t)

	const n = 30
	seed, err := outbox.New(pool, pool, nil, outbox.WithInstanceID("seed"))
	if err != nil {
		t.Fatalf("New seed: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := seed.Enqueue(ctx, outbox.Message{Topic: "t", Payload: []byte("x")}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	p1, p2 := &capturePublisher{}, &capturePublisher{}
	for _, tc := range []struct {
		id  string
		pub *capturePublisher
	}{{"i1", p1}, {"i2", p2}} {
		ob, err := outbox.New(pool, pool, tc.pub,
			outbox.WithInstanceID(tc.id),
			outbox.WithPollInterval(10*time.Millisecond),
			outbox.WithBatchSize(5),
		)
		if err != nil {
			t.Fatalf("New %s: %v", tc.id, err)
		}
		go func() { _ = ob.Run(ctx) }()
	}

	waitFor(t, 10*time.Second, func() bool {
		return len(p1.published())+len(p2.published()) == n
	})
	cancel()

	seen := map[string]bool{}
	for _, m := range append(p1.published(), p2.published()...) {
		if seen[m.ID] {
			t.Fatalf("message %s delivered twice", m.ID)
		}
		seen[m.ID] = true
	}
	if len(seen) != n {
		t.Fatalf("delivered %d unique, want %d", len(seen), n)
	}
}

// A permanently failing publisher moves the message to 'dead' after the attempt
// ceiling is reached.
func TestRetryThenDead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newTestPool(t)
	pub := &capturePublisher{failN: 1 << 30} // always fail
	ob, err := outbox.New(pool, pool, pub,
		outbox.WithInstanceID("dead"),
		outbox.WithPollInterval(10*time.Millisecond),
		outbox.WithMaxAttempts(1),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ob.Enqueue(ctx, outbox.Message{Topic: "t", Payload: []byte("x")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	go func() { _ = ob.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool {
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM outbox_messages LIMIT 1`).Scan(&status); err != nil {
			return false
		}
		return status == string(outbox.StatusDead)
	})
	cancel()
}

// Rows of one key written in one transaction share created_at; ordered mode
// must still deliver them in insertion order. It claims only the head row of a
// key per pass, so every pass is short: the relay must keep draining instead of
// sleeping PollInterval between rows.
func TestOrderedSingleKeyInsertionOrder(t *testing.T) {
	t.Parallel()
	const n = 50
	for _, tc := range []struct {
		name    string
		enqueue func(ctx context.Context, pool *pgxpool.Pool, msgs []outbox.Message) error
	}{
		{name: "one EnqueueBatch", enqueue: func(ctx context.Context, pool *pgxpool.Pool, msgs []outbox.Message) error {
			ob, err := outbox.New(pool, pool, nil)
			if err != nil {
				return err
			}
			return ob.EnqueueBatch(ctx, msgs)
		}},
		{name: "Enqueue calls in one transaction", enqueue: func(ctx context.Context, pool *pgxpool.Pool, msgs []outbox.Message) error {
			tx, err := pool.Begin(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback(ctx) }()
			ob, err := outbox.New(pool, tx, nil)
			if err != nil {
				return err
			}
			for _, m := range msgs {
				if err := ob.Enqueue(ctx, m); err != nil {
					return err
				}
			}
			return tx.Commit(ctx)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pool := newTestPool(t)

			msgs := make([]outbox.Message, n)
			for i := range msgs {
				msgs[i] = outbox.Message{Topic: "t", PartitionKey: "k", Payload: []byte{byte(i)}}
			}
			if err := tc.enqueue(ctx, pool, msgs); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			pub := &capturePublisher{}
			ob, err := outbox.New(pool, pool, pub,
				outbox.WithInstanceID("ordered"),
				outbox.WithOrdered(true),
				outbox.WithoutNotify(),
				outbox.WithPollInterval(time.Second),
			)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			go func() { _ = ob.Run(ctx) }()

			// One poll tick, then the whole key; one tick per row would take n seconds.
			waitFor(t, 5*time.Second, func() bool { return len(pub.published()) == n })
			for i, m := range pub.published() {
				if m.Payload[0] != byte(i) {
					t.Fatalf("row %d delivered at position %d", m.Payload[0], i)
				}
			}
		})
	}
}

// The cleaner deletes published rows older than the retention window.
func TestCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newTestPool(t)
	if _, err := pool.Exec(ctx,
		`INSERT INTO outbox_messages (topic, payload, status, published_at)
		 VALUES ('t', 'x', 'published', now() - interval '1 hour')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	ob, err := outbox.New(pool, pool, &capturePublisher{},
		outbox.WithInstanceID("clean"),
		outbox.WithPollInterval(time.Second),
		outbox.WithRetention(time.Minute),
		outbox.WithCleanupInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	go func() { _ = ob.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool {
		var c int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages`).Scan(&c); err != nil {
			return false
		}
		return c == 0
	})
	cancel()
}
