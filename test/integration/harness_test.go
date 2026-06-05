package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newTestPool starts a throwaway Postgres, applies the outbox migrations, and
// returns a connected pool. The container is terminated via t.Cleanup.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("outbox"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	for _, m := range outbox.Migrations() {
		if _, err := pool.Exec(ctx, m); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}
	return pool
}

// capturePublisher records every delivered message; it can be told to fail the
// first failN Publish calls.
type capturePublisher struct {
	mu    sync.Mutex
	calls int
	msgs  []outbox.Message
	failN int
}

func (p *capturePublisher) Publish(_ context.Context, msgs []outbox.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls <= p.failN {
		return errPublish
	}
	p.msgs = append(p.msgs, msgs...)
	return nil
}

func (p *capturePublisher) published() []outbox.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]outbox.Message(nil), p.msgs...)
}

var errPublish = errPublishT("transient publish failure")

type errPublishT string

func (e errPublishT) Error() string { return string(e) }

// waitFor polls cond until true or the deadline elapses.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(d)
	for !cond() {
		select {
		case <-deadline:
			t.Fatalf("condition not met within %s", d)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
