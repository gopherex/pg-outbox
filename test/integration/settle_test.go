package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedKeyed(t *testing.T, pool *pgxpool.Pool, rows ...[2]string) {
	t.Helper()
	ob, err := outbox.New(pool, pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	msgs := make([]outbox.Message, len(rows))
	for i, r := range rows {
		msgs[i] = outbox.Message{ID: r[0], Topic: r[0], PartitionKey: r[1], Payload: []byte("x")}
	}
	if err := ob.EnqueueBatch(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}
}

func rowState(t *testing.T, pool *pgxpool.Pool, id string) (status string, attempts int) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), `SELECT status, attempts FROM outbox_messages WHERE id = $1`, id).
		Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	return status, attempts
}

// komeet's head-of-line case: a slow row of partition A must not hold the rows
// of partition B that its batch already delivered. B settles early, its next
// head is claimed by another worker, and the wait for B ends while A is still
// publishing; A's lease stays renewed throughout (no reclaim).
func TestSettleReleasesOtherPartitions(t *testing.T) {
	pool := newTestPool(t)
	a1, b1, b2 := uid(1), uid(2), uid(3)
	seedKeyed(t, pool, [2]string{a1, "A"}, [2]string{b1, "B"}, [2]string{b2, "B"})

	gate := make(chan struct{})
	pub := publisherFunc(func(ctx context.Context, ms []outbox.Message) error {
		var slow bool
		for _, m := range ms {
			if m.PartitionKey == "A" {
				slow = true
				continue
			}
			if err := outbox.Settle(ctx, m, nil); err != nil {
				return err
			}
		}
		if slow {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	ob, err := outbox.New(pool, pool, pub, outbox.WithOrdered(true), outbox.WithConcurrency(4),
		outbox.WithBatchSize(100), outbox.WithLeaseDuration(600*time.Millisecond),
		outbox.WithPollInterval(10*time.Millisecond), outbox.WithWaitRecheck(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ob.Run(ctx) }()

	waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
	defer waitCancel()
	start := time.Now()
	if err := ob.WaitPublished(waitCtx, b1, b2); err != nil {
		t.Fatalf("partition B held behind A: %v", err)
	}
	t.Logf("partition B delivered in %s while A publishes", time.Since(start))
	if st, _ := rowState(t, pool, a1); st != "processing" {
		t.Fatalf("A1 is %s, want still processing", st)
	}

	time.Sleep(time.Second) // longer than A1's lease: only renewal keeps it ours
	close(gate)
	if err := ob.WaitPublished(waitCtx, a1); err != nil {
		t.Fatal(err)
	}
	if _, attempts := rowState(t, pool, a1); attempts != 1 {
		t.Fatalf("A1 attempts = %d, want 1 (lease renewed, never reclaimed)", attempts)
	}
}

// gatedPublisher holds partition A until gate closes and settles everything
// else early, as a per-message-ack publisher would.
func gatedPublisher(gate <-chan struct{}) publisherFunc {
	return func(ctx context.Context, ms []outbox.Message) error {
		var slow bool
		for _, m := range ms {
			if m.PartitionKey == "A" {
				slow = true
				continue
			}
			if err := outbox.Settle(ctx, m, nil); err != nil {
				return err
			}
		}
		if slow {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}

// A partition freed by an early Settle is claimed at once by an idle worker,
// of the same relay or of another instance, not at the next poll (an hour
// away here).
func TestSettleWakesIdleWorkers(t *testing.T) {
	for _, tc := range []struct {
		name      string
		instances int
		workers   int
	}{
		{name: "same relay", instances: 1, workers: 4},
		{name: "another instance", instances: 2, workers: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := newTestPool(t)
			gate := make(chan struct{})
			defer close(gate)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var obs []*outbox.Outbox
			for i := range tc.instances {
				ob, err := outbox.New(pool, pool, gatedPublisher(gate), outbox.WithOrdered(true),
					outbox.WithInstanceID(fmt.Sprintf("i%d", i)), outbox.WithConcurrency(tc.workers),
					outbox.WithBatchSize(100), outbox.WithPollInterval(time.Hour), outbox.WithWaitRecheck(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				obs = append(obs, ob)
				go func() { _ = ob.Run(ctx) }()
			}
			time.Sleep(200 * time.Millisecond) // listeners up: the enqueue NOTIFY wakes the first claim
			a1, b1, b2 := uid(1), uid(2), uid(3)
			seedKeyed(t, pool, [2]string{a1, "A"}, [2]string{b1, "B"}, [2]string{b2, "B"})

			waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
			defer waitCancel()
			start := time.Now()
			if err := obs[0].WaitPublished(waitCtx, b1, b2); err != nil {
				t.Fatalf("B2 not claimed after B1 settled: %v", err)
			}
			t.Logf("partition B delivered in %s", time.Since(start))
		})
	}
}

// Early outcomes other than published commit like PerMessage entries; rows
// not settled early take Publish's return.
func TestSettleOutcomes(t *testing.T) {
	pool := newTestPool(t)
	seedKeyed(t, pool, [2]string{uid(1), ""}, [2]string{uid(2), ""}, [2]string{uid(3), ""}, [2]string{uid(4), ""})
	refused := errors.New("refused")
	pub := publisherFunc(func(ctx context.Context, ms []outbox.Message) error {
		for _, m := range ms {
			var outcome error
			switch m.ID {
			case uid(1):
				outcome = outbox.Permanent(refused)
			case uid(2):
				outcome = outbox.Discard(refused)
			case uid(3):
				outcome = outbox.RetryAfter(refused, time.Hour)
			default:
				continue // takes the return
			}
			if err := outbox.Settle(ctx, m, outcome); err != nil {
				return err
			}
		}
		return nil
	})
	relay(t, pool, pub, outbox.WithBatchSize(10))
	for id, want := range map[string]string{uid(1): "dead", uid(2): "discarded", uid(3): "pending", uid(4): "published"} {
		waitFor(t, 5*time.Second, func() bool { st, _ := rowState(t, pool, id); return st == want })
	}
}

// Misuse is reported, never silently ignored.
func TestSettleMisuse(t *testing.T) {
	pool := newTestPool(t)
	seedKeyed(t, pool, [2]string{uid(1), ""})
	var (
		mu    sync.Mutex
		errs  []error
		saved context.Context
	)
	done := make(chan struct{})
	pub := publisherFunc(func(ctx context.Context, ms []outbox.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if saved != nil {
			return nil
		}
		saved = ctx
		m := ms[0]
		errs = append(errs,
			outbox.Settle(ctx, outbox.Message{ID: uid(9)}, nil), // not in the batch
			outbox.Settle(ctx, m, nil),
			outbox.Settle(ctx, m, nil), // twice
			outbox.Settle(context.Background(), m, nil),
		)
		close(done)
		return nil
	})
	relay(t, pool, pub)
	<-done
	waitFor(t, 5*time.Second, func() bool { st, _ := rowState(t, pool, uid(1)); return st == "published" })
	mu.Lock()
	defer mu.Unlock()
	errs = append(errs, outbox.Settle(saved, outbox.Message{ID: uid(1)}, nil)) // after Publish returned
	want := []error{outbox.ErrUnknownMessage, nil, outbox.ErrAlreadySettled, outbox.ErrNotSettling, outbox.ErrNotSettling}
	for i, w := range want {
		if !errors.Is(errs[i], w) || (w == nil) != (errs[i] == nil) {
			t.Fatalf("call %d: %v, want %v", i, errs[i], w)
		}
	}
}

// A fatal early outcome leaves its row claimed and stops the relay; rows
// settled before it stay committed.
func TestSettleFatal(t *testing.T) {
	pool := newTestPool(t)
	seedKeyed(t, pool, [2]string{uid(1), ""}, [2]string{uid(2), ""})
	errBroken := errors.New("broken")
	err := runToEnd(t, pool, publisherFunc(func(ctx context.Context, ms []outbox.Message) error {
		for _, m := range ms {
			outcome := error(nil)
			if m.ID == uid(2) {
				outcome = outbox.Abort(errBroken)
			}
			if err := outbox.Settle(ctx, m, outcome); err != nil {
				return err
			}
		}
		return nil
	}), outbox.WithBatchSize(10), outbox.WithConcurrency(1))
	if !errors.Is(err, errBroken) {
		t.Fatalf("Run = %v, want the abort", err)
	}
	if st, _ := rowState(t, pool, uid(1)); st != "published" {
		t.Fatalf("settled row is %s, want published", st)
	}
	if st, _ := rowState(t, pool, uid(2)); st != "processing" {
		t.Fatalf("aborted row is %s, want left claimed", st)
	}
}
