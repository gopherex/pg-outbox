package port

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gopherex/pg-outbox/message"
)

type ctxKey string

func TestMessageContext(t *testing.T) {
	t.Parallel()
	base := context.Background()
	own := context.WithValue(base, ctxKey("k"), "a")
	ctx := WithMessageContexts(base, map[string]context.Context{"a": own})
	for _, tc := range []struct {
		name string
		ctx  context.Context
		id   string
		want context.Context
	}{
		{name: "opened delivery", ctx: ctx, id: "a", want: own},
		{name: "message without a delivery", ctx: ctx, id: "b", want: ctx},
		{name: "context without deliveries", ctx: base, id: "a", want: base},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := MessageContext(tc.ctx, message.Message{ID: tc.id}); got != tc.want {
				t.Fatalf("MessageContext = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMessageTimeouts(t *testing.T) {
	t.Parallel()
	const budget = time.Hour
	for _, tc := range []struct {
		name     string
		timeout  time.Duration
		id       string
		deadline bool
		own      bool
	}{
		{name: "budget starts on first request", timeout: budget, id: "a", deadline: true},
		{name: "zero timeout keeps the delivery", id: "a", own: true},
		{name: "message without a delivery", timeout: budget, id: "b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := context.Background()
			own := context.WithValue(base, ctxKey("k"), "a")
			ctx, release := WithMessageTimeouts(base, map[string]context.Context{"a": own}, tc.timeout)
			before := time.Now()
			got := MessageContext(ctx, message.Message{ID: tc.id})
			deadline, ok := got.Deadline()
			switch {
			case ok != tc.deadline:
				t.Fatalf("deadline set = %v, want %v", ok, tc.deadline)
			case ok && deadline.Before(before.Add(tc.timeout)):
				t.Fatalf("deadline %v starts before the request at %v", deadline, before)
			case tc.own && got != own, !tc.own && !tc.deadline && got != ctx:
				t.Fatalf("MessageContext = %v", got)
			}
			if again := MessageContext(ctx, message.Message{ID: tc.id}); again != got {
				t.Fatal("a second request must return the same context and budget")
			}
			release()
			if tc.deadline && !errors.Is(got.Err(), context.Canceled) {
				t.Fatalf("after release Err = %v, want Canceled", got.Err())
			}
		})
	}
}

func TestMessageTimeoutsAreIndependent(t *testing.T) {
	t.Parallel()
	base := context.Background()
	ctx, release := WithMessageTimeouts(base, map[string]context.Context{"a": base, "b": base}, 30*time.Millisecond)
	defer release()
	first := MessageContext(ctx, message.Message{ID: "a"})
	<-first.Done()
	second := MessageContext(ctx, message.Message{ID: "b"})
	if second.Err() != nil {
		t.Fatalf("the second message lost its budget to the first: %v", second.Err())
	}
	<-second.Done()
	if !errors.Is(second.Err(), context.DeadlineExceeded) {
		t.Fatalf("Err = %v, want DeadlineExceeded", second.Err())
	}
}

func BenchmarkMessageContext(b *testing.B) {
	base := context.Background()
	ctx, release := WithMessageTimeouts(base, map[string]context.Context{"a": base}, time.Hour)
	defer release()
	m := message.Message{ID: "a"}
	for b.Loop() {
		MessageContext(ctx, m)
	}
}

func TestNoopInstrumentation(t *testing.T) {
	t.Parallel()
	var i Instrumentation = NoopInstrumentation{}
	headers := map[string]string{}
	i.Capture(context.Background(), headers)
	if len(headers) != 0 {
		t.Fatalf("Capture wrote %v", headers)
	}
	ctx := context.WithValue(context.Background(), ctxKey("k"), "v")
	got, end := i.Deliver(ctx, message.Message{ID: "a"})
	if got != ctx {
		t.Fatal("Deliver must return the batch context")
	}
	end(Outcome{Status: message.StatusPublished})
}
