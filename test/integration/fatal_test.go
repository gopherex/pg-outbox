package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// runToEnd runs a relay until it stops by itself and returns Run's error.
func runToEnd(t *testing.T, pool *pgxpool.Pool, pub outbox.Publisher, opts ...outbox.Option) error {
	t.Helper()
	options := append([]outbox.Option{outbox.WithPollInterval(10 * time.Millisecond), outbox.WithBatchSize(1), outbox.WithoutNotify(), outbox.WithConcurrency(2)}, opts...)
	ob, err := outbox.New(pool, pool, pub, options...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = ob.Run(ctx)
	if ctx.Err() != nil {
		t.Fatalf("relay did not stop on a fatal error: %v", err)
	}
	return err
}

func TestFatalStopLeavesBatchClaimed(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	errBroken := errors.New("publisher state corrupted")
	for _, tc := range []struct {
		name  string
		pub   publisherFunc
		opts  []outbox.Option
		check func(error) bool
	}{
		{
			name: "panic",
			pub:  func(context.Context, []outbox.Message) error { panic(errBroken) },
			check: func(err error) bool {
				var p *outbox.PanicError
				return errors.As(err, &p) && errors.Is(err, errBroken) && strings.Contains(string(p.Stack), "fatal_test.go")
			},
		},
		{
			name: "abort above permanent",
			pub: func(context.Context, []outbox.Message) error {
				return fmt.Errorf("wrapped: %w", outbox.Permanent(outbox.Abort(errBroken)))
			},
			check: func(err error) bool {
				var a *outbox.AbortError
				return errors.As(err, &a) && errors.Is(err, errBroken)
			},
		},
		{
			name: "abort in a per-message outcome",
			pub: func(_ context.Context, ms []outbox.Message) error {
				return outbox.PerMessage(map[string]error{ms[0].ID: outbox.Abort(errBroken)}, nil)
			},
			check: func(err error) bool {
				var a *outbox.AbortError
				return errors.As(err, &a) && errors.Is(err, errBroken)
			},
		},
		{
			name: "classified publish error",
			pub:  func(context.Context, []outbox.Message) error { return errBroken },
			opts: []outbox.Option{outbox.WithFatal(func(err error) bool { return errors.Is(err, errBroken) })},
			check: func(err error) bool {
				return errors.Is(err, errBroken)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := "fatal-" + tc.name
			if _, err := pool.Exec(ctx, "INSERT INTO outbox_messages(topic,payload) VALUES ($1,'x')", topic); err != nil {
				t.Fatal(err)
			}
			hooks := &outcomeHooks{}
			opts := append([]outbox.Option{outbox.WithLeaseDuration(time.Second), outbox.WithHooks(hooks), outbox.WithMaxAttempts(1)}, tc.opts...)
			if err := runToEnd(t, pool, tc.pub, opts...); !tc.check(err) {
				t.Fatalf("Run = %v", err)
			}
			var claimed bool
			if err := pool.QueryRow(ctx, "SELECT status='processing' AND attempts=1 AND lease_token IS NOT NULL AND finished_at IS NULL AND last_error IS NULL FROM outbox_messages WHERE topic=$1", topic).Scan(&claimed); err != nil {
				t.Fatal(err)
			}
			if !claimed || hooks.published.Load()+hooks.discarded.Load()+hooks.failed.Load()+hooks.dead.Load() != 0 {
				t.Fatal("fatal stop completed, retried or dead-lettered the batch")
			}

			// A second relay re-claims the row once the abandoned lease expires.
			got := &capturePublisher{}
			_, stop := relay(t, pool, got)
			waitStatus(t, pool, topic, "published")
			stop()
			var attempts int
			if err := pool.QueryRow(ctx, "SELECT attempts FROM outbox_messages WHERE topic=$1", topic).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			if len(got.published()) != 1 || attempts != 2 {
				t.Fatalf("re-claim delivered %d messages, attempts=%d", len(got.published()), attempts)
			}
		})
	}
}

func TestFatalInternalError(t *testing.T) {
	pool := newTestPool(t)
	err := runToEnd(t, pool, publisherFunc(func(context.Context, []outbox.Message) error { return nil }),
		outbox.WithSchema("missing"),
		outbox.WithFatal(func(err error) bool {
			var pg *pgconn.PgError
			return errors.As(err, &pg) && pg.Code == "42P01" // undefined_table
		}))
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "42P01" {
		t.Fatalf("Run = %v, want undefined_table", err)
	}
}
