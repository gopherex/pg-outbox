package port

import (
	"context"
	"testing"

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
