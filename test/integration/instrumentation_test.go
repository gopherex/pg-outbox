package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
)

type originKey struct{}

type deliveryKey struct{}

// recorder is an Instrumentation that captures an origin value from the
// enqueue context and records every delivery's outcome.
type recorder struct {
	mu   sync.Mutex
	ends map[string][]outbox.Outcome
}

func (r *recorder) Capture(ctx context.Context, headers map[string]string) {
	if v, ok := ctx.Value(originKey{}).(string); ok {
		headers["x-origin"] = v
		headers["x-shared"] = "instrumentation"
	}
	if id, ok := ctx.Value(deliveryKey{}).(string); ok {
		headers["x-delivery"] = id
	}
}

func (r *recorder) Deliver(ctx context.Context, m outbox.Message) (context.Context, func(outbox.Outcome)) {
	return context.WithValue(ctx, deliveryKey{}, m.ID), func(o outbox.Outcome) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.ends[m.Topic] = append(r.ends[m.Topic], o)
	}
}

func (r *recorder) outcomes() map[string][]outbox.Outcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][]outbox.Outcome, len(r.ends))
	for k, v := range r.ends {
		out[k] = append([]outbox.Outcome(nil), v...)
	}
	return out
}

// Capture's headers are stored with the row; the caller's own entries win and
// the caller's map is never written.
func TestInstrumentationCapture(t *testing.T) {
	pool := newTestPool(t)
	rec := &recorder{ends: map[string][]outbox.Outcome{}}
	ob, err := outbox.New(pool, pool, nil, outbox.WithInstrumentation(rec), outbox.WithoutNotify())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), originKey{}, "trace-1")
	callerHeaders := map[string]string{"x-shared": "caller"}
	if err := ob.Enqueue(ctx, outbox.Message{Topic: "t", Payload: []byte("x"), Headers: callerHeaders}); err != nil {
		t.Fatal(err)
	}
	if err := ob.Enqueue(context.Background(), outbox.Message{Topic: "plain", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if len(callerHeaders) != 1 {
		t.Fatalf("caller headers were written: %v", callerHeaders)
	}
	for topic, want := range map[string]map[string]string{
		"t":     {"x-origin": "trace-1", "x-shared": "caller"},
		"plain": {},
	} {
		var raw []byte
		if err := pool.QueryRow(ctx, `SELECT headers FROM outbox_messages WHERE topic = $1`, topic).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var got map[string]string
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) || got["x-origin"] != want["x-origin"] || got["x-shared"] != want["x-shared"] {
			t.Fatalf("%s headers = %v, want %v", topic, got, want)
		}
	}
}

// Every delivery is opened before Publish, reachable through MessageContext
// with the publish deadline, and ended once with its row's outcome.
func TestInstrumentationDeliveries(t *testing.T) {
	pool := newTestPool(t)
	rec := &recorder{ends: map[string][]outbox.Outcome{}}
	refused := errors.New("refused")
	var ctxErrs []error
	var mu sync.Mutex
	pub := publisherFunc(func(ctx context.Context, ms []outbox.Message) error {
		outcomes := map[string]error{}
		for _, m := range ms {
			mctx := outbox.MessageContext(ctx, m)
			mu.Lock()
			if id, _ := mctx.Value(deliveryKey{}).(string); id != m.ID {
				ctxErrs = append(ctxErrs, errors.New("message context of "+m.Topic+" carries "+id))
			}
			if _, ok := mctx.Deadline(); !ok {
				ctxErrs = append(ctxErrs, errors.New("message context of "+m.Topic+" lost the publish deadline"))
			}
			if m.Headers["x-delivery"] != m.ID {
				ctxErrs = append(ctxErrs, errors.New("headers of "+m.Topic+" do not carry its delivery"))
			}
			mu.Unlock()
			switch m.Topic {
			case "permanent":
				outcomes[m.ID] = outbox.Permanent(refused)
			case "discard":
				outcomes[m.ID] = outbox.Discard(refused)
			case "retry":
				outcomes[m.ID] = outbox.RetryAfter(refused, time.Hour)
			}
		}
		return outbox.PerMessage(outcomes, nil)
	})
	ob, err := outbox.New(pool, pool, pub, outbox.WithInstrumentation(rec), outbox.WithoutNotify(),
		outbox.WithBatchSize(10), outbox.WithPollInterval(10*time.Millisecond), outbox.WithPublishTimeout(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var msgs []outbox.Message
	for _, topic := range []string{"ok", "permanent", "discard", "retry"} {
		msgs = append(msgs, outbox.Message{Topic: topic, Payload: []byte("x")})
	}
	if err := ob.EnqueueBatch(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	go func() { _ = ob.Run(ctx) }()

	want := map[string]outbox.Status{"ok": outbox.StatusPublished, "permanent": outbox.StatusDead,
		"discard": outbox.StatusDiscarded, "retry": outbox.StatusPending}
	waitFor(t, 5*time.Second, func() bool { return len(rec.outcomes()) == len(want) })
	time.Sleep(100 * time.Millisecond) // a second end would show up here
	for topic, status := range want {
		got := rec.outcomes()[topic]
		if len(got) != 1 || got[0].Status != status || (status == outbox.StatusPublished) != (got[0].Err == nil) {
			t.Fatalf("%s outcomes = %v, want one %s", topic, got, status)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ctxErrs) > 0 {
		t.Fatal(errors.Join(ctxErrs...))
	}
	// The delivery headers go to the transport only, never into the row.
	var stored int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE headers ? 'x-delivery'`).Scan(&stored); err != nil || stored != 0 {
		t.Fatalf("%d rows store delivery headers (%v)", stored, err)
	}
}

// An aborted batch stays claimed: its deliveries end as processing with the
// abort as cause.
func TestInstrumentationAbort(t *testing.T) {
	pool := newTestPool(t)
	rec := &recorder{ends: map[string][]outbox.Outcome{}}
	errBroken := errors.New("broken")
	if _, err := pool.Exec(context.Background(), "INSERT INTO outbox_messages(topic,payload) VALUES ('aborted','x')"); err != nil {
		t.Fatal(err)
	}
	err := runToEnd(t, pool, publisherFunc(func(context.Context, []outbox.Message) error {
		return outbox.Abort(errBroken)
	}), outbox.WithInstrumentation(rec))
	if !errors.Is(err, errBroken) {
		t.Fatalf("Run = %v", err)
	}
	got := rec.outcomes()["aborted"]
	var abort *outbox.AbortError
	if len(got) != 1 || got[0].Status != outbox.StatusProcessing || !errors.As(got[0].Err, &abort) {
		t.Fatalf("aborted outcomes = %v, want one processing with the abort", got)
	}
}
