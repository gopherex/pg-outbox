package port

import (
	"context"
	"errors"

	"github.com/gopherex/pg-outbox/message"
)

var (
	// ErrNotSettling is returned by Settle outside a relay Publish call, or
	// after that call returned.
	ErrNotSettling = errors.New("outbox: Settle outside a relay Publish call")
	// ErrUnknownMessage is returned by Settle for a message not in the batch.
	ErrUnknownMessage = errors.New("outbox: Settle for a message not in this batch")
	// ErrAlreadySettled is returned by a second Settle for the same message.
	ErrAlreadySettled = errors.New("outbox: message already settled")
)

// Settler records the outcome of one message of the batch being published.
// The relay puts one into the Publish context; publishers reach it through
// Settle.
type Settler interface {
	Settle(m message.Message, outcome error) error
}

type settlerKey struct{}

// WithSettler returns ctx carrying the relay's Settler for one Publish call.
func WithSettler(ctx context.Context, s Settler) context.Context {
	return context.WithValue(ctx, settlerKey{}, s)
}

// Settle reports the outcome of message m as soon as it is known, while the
// rest of the batch is still being published: nil means published; any other
// outcome is read exactly like a PerMessage entry (Permanent, Discard,
// RetryAfter, a plain error that retries). The relay commits it right away
// (group-committed with other early outcomes), so the row stops holding its
// partition and the next row of that partition can be claimed by another
// worker. A fatal outcome (Abort, or one WithFatal accepts) leaves the row
// claimed and stops the relay once Publish returns.
//
// Messages a publisher does not settle take the outcome Publish returns, as
// before. ctx must be the Publish context (or derived from it, such as
// MessageContext); m is identified by Message.ID.
func Settle(ctx context.Context, m message.Message, outcome error) error {
	s, ok := ctx.Value(settlerKey{}).(Settler)
	if !ok {
		return ErrNotSettling
	}
	return s.Settle(m, outcome)
}
