package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gopherex/pg-outbox/config"
	"github.com/gopherex/pg-outbox/message"
	"github.com/gopherex/pg-outbox/port"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestNewStoreQualifiesTable(t *testing.T) {
	s := NewStore(nil, nil, nil, "events", false)
	if s.Table() != `"events".outbox_messages` {
		t.Fatalf("table = %q, want \"events\".outbox_messages", s.Table())
	}
}

// testWaiters is a registry for relays whose tests never wait.
func testWaiters() *Waiters { return NewWaiters("public", newFake().read, time.Hour, false) }

type mutatePublisher struct{}

func (mutatePublisher) Publish(_ context.Context, msgs []message.Message) error {
	msgs[0].ID = "adapter-id"
	msgs[0].LeaseToken = "adapter-token"
	msgs[0].Attempts = 999
	return nil
}

func TestPublisherCannotChangeClaimIdentity(t *testing.T) {
	r := NewRelay(NewStore(nil, nil, nil, "public", false), mutatePublisher{}, config.Resolve(), testWaiters())
	msgs := []message.Message{{ID: "claim-id", LeaseToken: "claim-token", Attempts: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := newSettler(r, ctx, msgs, outcomes{}, map[string]func(port.Outcome){})
	go st.run()
	defer st.close()
	if err, leaseErr := r.publish(ctx, cancel, st, append([]message.Message(nil), msgs...)); err != nil || leaseErr != nil {
		t.Fatalf("publish: %v, %v", err, leaseErr)
	}
	if msgs[0].ID != "claim-id" || msgs[0].LeaseToken != "claim-token" || msgs[0].Attempts != 1 {
		t.Fatalf("publisher changed private claim identity: %+v", msgs[0])
	}
	if c := st.claims["claim-id"]; c.LeaseToken != "claim-token" || c.Attempts != 1 {
		t.Fatalf("publisher changed the settler's claim: %+v", c)
	}
}

// failExec fails or panics on every query, standing in for a broken database.
type failExec struct {
	err    error
	panics bool
}

func (e failExec) fail() error {
	if e.panics {
		panic(e.err)
	}
	return e.err
}

func (e failExec) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, e.fail()
}
func (e failExec) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, e.fail() }
func (e failExec) QueryRow(context.Context, string, ...any) pgx.Row        { return nil }

func TestRunStopsOnFatal(t *testing.T) {
	t.Parallel()
	errDB := errors.New("db down")
	for _, tc := range []struct {
		name  string
		exec  failExec
		fatal func(error) bool
	}{
		{name: "classified internal error", exec: failExec{err: errDB}, fatal: func(err error) bool { return errors.Is(err, errDB) }},
		{name: "abort from executor", exec: failExec{err: port.Abort(errDB)}},
		{name: "panic", exec: failExec{err: errDB, panics: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := config.Resolve(config.WithPollInterval(time.Millisecond), config.WithoutNotify(), config.WithConcurrency(3), config.WithFatal(tc.fatal))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := NewRelay(NewStore(tc.exec, nil, nil, "public", false), nil, set, testWaiters()).Run(ctx)
			if !errors.Is(err, errDB) || ctx.Err() != nil {
				t.Fatalf("Run = %v (ctx %v), want fatal %v", err, ctx.Err(), errDB)
			}
			var panicked *port.PanicError
			if errors.As(err, &panicked) != tc.exec.panics || tc.exec.panics && !strings.Contains(string(panicked.Stack), "failExec") {
				t.Fatalf("panic error = %#v", err)
			}
		})
	}
}

func TestRunIgnoresUnclassifiedErrors(t *testing.T) {
	t.Parallel()
	set := config.Resolve(config.WithPollInterval(time.Millisecond), config.WithoutNotify())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := NewRelay(NewStore(failExec{err: errors.New("transient")}, nil, nil, "public", false), nil, set, testWaiters()).Run(ctx); err != nil {
		t.Fatalf("Run = %v, want nil after cancellation", err)
	}
}

func TestCleanerStopsOnFatal(t *testing.T) {
	t.Parallel()
	errDB := errors.New("db down")
	for _, tc := range []struct {
		name string
		exec failExec
	}{
		{name: "classified", exec: failExec{err: errDB}},
		{name: "panic", exec: failExec{err: errDB, panics: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := config.Resolve(config.WithCleanupInterval(time.Millisecond), config.WithRetention(time.Hour),
				config.WithFatal(func(err error) bool { return errors.Is(err, errDB) }))
			err := NewCleaner(NewStore(tc.exec, nil, nil, "public", false), set).Run(context.Background())
			var panicked *port.PanicError
			if !errors.Is(err, errDB) || errors.As(err, &panicked) != tc.exec.panics {
				t.Fatalf("Run = %v", err)
			}
		})
	}
}

func TestIsFatal(t *testing.T) {
	t.Parallel()
	errX := errors.New("x")
	done, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()
	always := config.Resolve(config.WithFatal(func(error) bool { return true }))
	never := config.Resolve()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		set  config.Settings
		err  error
		want bool
	}{
		{"plain default", live, never, errX, false},
		{"classifier", live, always, errX, true},
		{"classifier ignored after shutdown", done, always, errX, false},
		{"abort wrapped", done, never, fmt.Errorf("w: %w", port.Abort(errX)), true},
		{"abort above permanent", live, never, port.Permanent(port.Abort(errX)), true},
		{"panic", done, never, &port.PanicError{Value: "boom"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isFatal(tc.ctx, tc.set, tc.err); got != tc.want {
				t.Fatalf("isFatal = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRecovered(t *testing.T) {
	t.Parallel()
	errX := errors.New("x")
	if err := recovered(func() error { return errX }); !errors.Is(err, errX) {
		t.Fatalf("passthrough = %v", err)
	}
	err := recovered(func() error { panic(errX) })
	var panicked *port.PanicError
	if !errors.As(err, &panicked) || !errors.Is(err, errX) || len(panicked.Stack) == 0 {
		t.Fatalf("recovered = %#v", err)
	}
}

func BenchmarkIsFatal(b *testing.B) {
	set := config.Resolve()
	err := fmt.Errorf("w: %w", errors.New("x"))
	ctx := context.Background()
	for b.Loop() {
		isFatal(ctx, set, err)
	}
}
