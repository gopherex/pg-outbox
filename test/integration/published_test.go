package integration

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// uid builds a deterministic uuid for test row n.
func uid(n int) string { return fmt.Sprintf("0190a5f0-0000-7000-8000-%012d", n) }

// insertRow writes a row with a given status directly, as another process's
// relay would have left it. A processing row holds a long lease so no relay
// in the test claims it.
func insertRow(t *testing.T, pool *pgxpool.Pool, id string, status outbox.Status) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO outbox_messages(id, topic, payload, status, locked_by, locked_until)
 VALUES ($1, 't', 'x', $2, CASE WHEN $2 = 'processing' THEN 'elsewhere' END,
 CASE WHEN $2 = 'processing' THEN now() + interval '1 hour' END)`, id, string(status)); err != nil {
		t.Fatal(err)
	}
}

func waitAsync(ctx context.Context, ob *outbox.Outbox, ids ...string) <-chan error {
	res := make(chan error, 1)
	go func() { res <- ob.WaitPublished(ctx, ids...) }()
	return res
}

func requireWaiting(t *testing.T, res <-chan error) {
	t.Helper()
	select {
	case err := <-res:
		t.Fatalf("wait finished early: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}

func requireDone(t *testing.T, res <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-res:
		return err
	case <-time.After(within):
		t.Fatalf("wait did not finish within %s", within)
		return nil
	}
}

func TestPublishedStates(t *testing.T) {
	pool := newTestPool(t)
	ob, err := outbox.New(pool, pool, nil, outbox.WithoutNotify())
	if err != nil {
		t.Fatal(err)
	}
	const (
		pending = iota + 1
		processing
		published
		dead
		discarded
		absent
	)
	for n, st := range map[int]outbox.Status{pending: outbox.StatusPending, processing: outbox.StatusProcessing,
		published: outbox.StatusPublished, dead: outbox.StatusDead, discarded: outbox.StatusDiscarded} {
		insertRow(t, pool, uid(n), st)
	}
	for _, tc := range []struct {
		name        string
		ids         []string
		want        bool
		undelivered []string
		err         error
	}{
		{name: "none", want: true},
		{name: "published", ids: []string{uid(published)}, want: true},
		{name: "absent is delivered", ids: []string{uid(absent)}, want: true},
		{name: "upper case id", ids: []string{strings.ToUpper(uid(published))}, want: true},
		{name: "pending", ids: []string{uid(published), uid(pending)}},
		{name: "processing", ids: []string{uid(processing)}},
		{name: "dead", ids: []string{uid(dead), uid(published)}, undelivered: []string{uid(dead)}},
		{name: "undelivered wins over pending", ids: []string{uid(pending), uid(discarded), uid(dead)},
			undelivered: []string{uid(dead), uid(discarded)}},
		{name: "not a uuid", ids: []string{"order-42"}, err: outbox.ErrInvalidID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ob.Published(context.Background(), tc.ids...)
			var u *outbox.UndeliveredError
			switch {
			case tc.err != nil:
				if !errors.Is(err, tc.err) {
					t.Fatalf("err = %v, want %v", err, tc.err)
				}
			case tc.undelivered != nil:
				if !errors.As(err, &u) || !errors.Is(err, outbox.ErrUndelivered) || !slices.Equal(u.IDs, tc.undelivered) {
					t.Fatalf("err = %v, want undelivered %v", err, tc.undelivered)
				}
			case err != nil || got != tc.want:
				t.Fatalf("Published = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

// While the cleaner also removes refused rows, a gone row's outcome is
// unknown: Published and WaitPublished say so instead of claiming delivery.
func TestPublishedWithRefusalsCollected(t *testing.T) {
	pool := newTestPool(t)
	ob, err := outbox.New(pool, pool, nil, outbox.WithoutNotify(),
		outbox.WithDeadRetention(time.Hour), outbox.WithCleanupInterval(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	insertRow(t, pool, uid(1), outbox.StatusPublished)
	insertRow(t, pool, uid(2), outbox.StatusDead)
	ctx := context.Background()

	if done, err := ob.Published(ctx, uid(1)); !done || err != nil {
		t.Fatalf("present published row: %v, %v", done, err)
	}
	var unknown *outbox.UnknownOutcomeError
	if _, err := ob.Published(ctx, uid(1), uid(3)); !errors.As(err, &unknown) ||
		!errors.Is(err, outbox.ErrOutcomeUnknown) || !slices.Equal(unknown.IDs, []string{uid(3)}) {
		t.Fatalf("gone row: %v, want ErrOutcomeUnknown for it", err)
	}
	if _, err := ob.Published(ctx, uid(2), uid(3)); !errors.Is(err, outbox.ErrUndelivered) {
		t.Fatalf("dead + gone: %v, want ErrUndelivered (certain) first", err)
	}
	if err := ob.WaitPublished(ctx, uid(3)); !errors.Is(err, outbox.ErrOutcomeUnknown) {
		t.Fatalf("wait on a gone row: %v, want ErrOutcomeUnknown", err)
	}
}

// v7Now builds a UUIDv7 created now (n tells rows apart).
func v7Now(n int) string {
	ms := uint64(time.Now().UnixMilli())
	return fmt.Sprintf("%08x-%04x-7000-8000-%012d", ms>>16, ms&0xffff, n)
}

// The cleaner removes refused rows but keeps their outcome: answers stay exact
// within the tombstone horizon, and once tombstones expire too, unknown.
func TestTombstonesKeepRefusals(t *testing.T) {
	pool := newTestPool(t)
	dead, discarded, published := v7Now(1), v7Now(2), v7Now(3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refused := errors.New("refused")
	pub := publisherFunc(func(_ context.Context, ms []outbox.Message) error {
		return outbox.PerMessage(map[string]error{dead: outbox.Permanent(refused), discarded: outbox.Discard(refused)}, nil)
	})
	retention := []outbox.Option{outbox.WithRetention(time.Millisecond), outbox.WithDeadRetention(time.Millisecond),
		outbox.WithDiscardRetention(time.Millisecond), outbox.WithCleanupInterval(20 * time.Millisecond)}
	ob, err := outbox.New(pool, pool, pub, append(retention, outbox.WithBatchSize(10),
		outbox.WithPollInterval(10*time.Millisecond))...)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{dead, discarded, published} {
		if err := ob.Enqueue(ctx, outbox.Message{ID: id, Topic: "t", Payload: []byte("payload")}); err != nil {
			t.Fatal(err)
		}
	}
	runCtx, stop := context.WithCancel(ctx)
	go func() { _ = ob.Run(runCtx) }()
	waitFor(t, 10*time.Second, func() bool {
		var rows int
		return pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages`).Scan(&rows) == nil && rows == 0
	})
	stop()

	var tombs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_tombstones WHERE id = ANY($1::uuid[])`,
		[]string{dead, discarded}).Scan(&tombs); err != nil || tombs != 2 {
		t.Fatalf("tombstones = %d, %v; want the dead and the discarded row", tombs, err)
	}
	var undelivered *outbox.UndeliveredError
	if _, err := ob.Published(ctx, dead, discarded); !errors.As(err, &undelivered) || len(undelivered.IDs) != 2 {
		t.Fatalf("removed refusals: %v, want ErrUndelivered for both", err)
	}
	if done, err := ob.Published(ctx, published); !done || err != nil {
		t.Fatalf("removed published v7 row: %v, %v; want delivered", done, err)
	}
	if err := ob.WaitPublished(ctx, published); err != nil {
		t.Fatalf("wait on the removed published row: %v", err)
	}
	if _, err := ob.Published(ctx, uid(9)); !errors.Is(err, outbox.ErrOutcomeUnknown) {
		t.Fatalf("gone v7 id past the horizon: %v, want ErrOutcomeUnknown", err)
	}

	// Tombstones expire: no false success afterwards, the answer is unknown.
	expired, err := outbox.New(pool, pool, nil, append(retention, outbox.WithTombstoneRetention(time.Millisecond))...)
	if err != nil {
		t.Fatal(err)
	}
	cleanCtx, stopClean := context.WithCancel(ctx)
	go func() { _ = expired.Run(cleanCtx) }()
	waitFor(t, 10*time.Second, func() bool {
		var rows int
		return pool.QueryRow(ctx, `SELECT count(*) FROM outbox_tombstones`).Scan(&rows) == nil && rows == 0
	})
	stopClean()
	if _, err := expired.Published(ctx, dead); !errors.Is(err, outbox.ErrOutcomeUnknown) {
		t.Fatalf("refusal whose tombstone expired: %v, want ErrOutcomeUnknown", err)
	}
}

// The wait is settled by the relay's commit notification: the recheck is an
// hour away, so nothing else could end it. A key stuck behind a failing
// publish does not hold it.
func TestWaitPublishedByNotification(t *testing.T) {
	pool := newTestPool(t)
	release := make(chan struct{})
	ids := []string{uid(1), uid(2), uid(3)}
	ob, err := outbox.New(pool, pool, publisherFunc(func(ctx context.Context, ms []outbox.Message) error {
		outcomes := map[string]error{}
		for _, m := range ms {
			if m.PartitionKey == "stuck" {
				outcomes[m.ID] = outbox.RetryAfter(errors.New("busy"), time.Hour)
				continue
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return outbox.PerMessage(outcomes, nil)
	}), outbox.WithPollInterval(10*time.Millisecond), outbox.WithWaitRecheck(time.Hour), outbox.WithBatchSize(10))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ob.Run(ctx) }()

	msgs := []outbox.Message{{ID: uid(9), Topic: "t", PartitionKey: "stuck", Payload: []byte("x")}}
	for _, id := range ids {
		msgs = append(msgs, outbox.Message{ID: id, Topic: "t", PartitionKey: "free", Payload: []byte("x")})
	}
	if err := ob.EnqueueBatch(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	res := waitAsync(ctx, ob, ids...)
	requireWaiting(t, res)
	if done, err := ob.Published(ctx, ids...); done || err != nil {
		t.Fatalf("Published before release = %v, %v", done, err)
	}

	start := time.Now()
	close(release)
	if err := requireDone(t, res, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	t.Logf("settled %s after publish", time.Since(start))
	if done, err := ob.Published(ctx, uid(9)); done || err != nil {
		t.Fatalf("stuck row: Published = %v, %v; want still pending", done, err)
	}
}

// A notification from any process settles the wait; one for another schema
// with the same id does not. A dead notification fails it.
func TestWaitPublishedForeignNotification(t *testing.T) {
	pool := newTestPool(t)
	ob, err := outbox.New(pool, pool, &capturePublisher{}, outbox.WithWaitRecheck(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ob.Run(ctx) }()
	insertRow(t, pool, uid(1), outbox.StatusProcessing)
	insertRow(t, pool, uid(2), outbox.StatusProcessing)

	published := waitAsync(ctx, ob, uid(1))
	failed := waitAsync(ctx, ob, uid(2))
	requireWaiting(t, published)

	notify := func(payload string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `SELECT pg_notify('outbox_messages_done', $1)`, payload); err != nil {
			t.Fatal(err)
		}
	}
	notify("other:published:" + uid(1))
	requireWaiting(t, published)

	notify("public:published:" + uid(1))
	if err := requireDone(t, published, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	notify("public:dead:" + uid(2))
	if err := requireDone(t, failed, 5*time.Second); !errors.Is(err, outbox.ErrUndelivered) {
		t.Fatalf("dead row: %v, want ErrUndelivered", err)
	}
}

// The relay announces exactly the rows it completed, per terminal status, in
// chunks that fit the payload limit.
func TestDoneNotificationsFromRelay(t *testing.T) {
	pool := newTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, "LISTEN outbox_messages_done"); err != nil {
		t.Fatal(err)
	}

	const n = 250
	deadID := uid(7)
	ob, err := outbox.New(pool, pool, publisherFunc(func(_ context.Context, _ []outbox.Message) error {
		return outbox.PerMessage(map[string]error{deadID: outbox.Permanent(errors.New("refused"))}, nil)
	}), outbox.WithBatchSize(n), outbox.WithPollInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	msgs := make([]outbox.Message, n)
	for i := range msgs {
		msgs[i] = outbox.Message{ID: uid(i), Topic: "t", Payload: []byte("x")}
	}
	if err := ob.EnqueueBatch(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = ob.Run(runCtx) }()

	seen := map[string]string{}
	chunks := 0
	for len(seen) < n {
		note, err := conn.WaitForNotification(ctx)
		if err != nil {
			t.Fatalf("after %d of %d ids: %v", len(seen), n, err)
		}
		parts := strings.SplitN(note.Payload, ":", 3)
		if len(parts) != 3 || parts[0] != "public" {
			t.Fatalf("payload %q", note.Payload)
		}
		ids := strings.Split(parts[2], ",")
		if len(ids) > 200 || len(note.Payload) >= 8000 {
			t.Fatalf("chunk of %d ids, %d bytes", len(ids), len(note.Payload))
		}
		chunks++
		for _, id := range ids {
			if prev, dup := seen[id]; dup {
				t.Fatalf("id %s announced twice (%s, %s)", id, prev, parts[1])
			}
			seen[id] = parts[1]
		}
	}
	for i := range n {
		want := "published"
		if uid(i) == deadID {
			want = "dead"
		}
		if seen[uid(i)] != want {
			t.Fatalf("id %s announced %q, want %q", uid(i), seen[uid(i)], want)
		}
	}
	if chunks != 3 { // 249 published in 200 + 49, 1 dead
		t.Fatalf("%d notifications, want 3", chunks)
	}
}

// Without a listener the shared recheck settles waits: a row published
// silently, a row the cleaner removed, and a deadline that passes first.
func TestWaitPublishedWithoutListener(t *testing.T) {
	pool := newTestPool(t)
	ob, err := outbox.New(pool, pool, nil, outbox.WithoutNotify(), outbox.WithWaitRecheck(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		insertRow(t, pool, uid(i), outbox.StatusPending)
	}
	published := waitAsync(ctx, ob, uid(1))
	removed := waitAsync(ctx, ob, uid(2))
	requireWaiting(t, published)

	if _, err := pool.Exec(ctx, `UPDATE outbox_messages SET status = 'published' WHERE id = $1`, uid(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM outbox_messages WHERE id = $1`, uid(2)); err != nil {
		t.Fatal(err)
	}
	if err := requireDone(t, published, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := requireDone(t, removed, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := ob.WaitPublished(short, uid(3)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pending row: %v, want the deadline", err)
	}
	if err := ob.WaitPublished(ctx, "order-42"); !errors.Is(err, outbox.ErrInvalidID) {
		t.Fatalf("bad id: %v, want ErrInvalidID", err)
	}
}
