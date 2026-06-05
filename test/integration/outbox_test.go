package integration

import (
	"context"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
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
