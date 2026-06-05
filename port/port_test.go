package port

import (
	"context"
	"errors"
	"testing"

	"github.com/gopherex/pg-outbox/message"
)

// NoopHooks must satisfy Hooks and never panic.
func TestNoopHooks(t *testing.T) {
	var h Hooks = NoopHooks{}
	ctx := context.Background()
	h.OnPublished(ctx, []message.Message{{Topic: "t"}})
	h.OnFailed(ctx, message.Message{Topic: "t"}, errors.New("boom"))
	h.OnDead(ctx, message.Message{Topic: "t"})
	h.OnCleanup(ctx, 3)
}
