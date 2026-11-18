package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
	natsgo "github.com/nats-io/nats.go"
)

type fakeClient struct {
	msgs     []*natsgo.Msg
	pubErr   error
	flushes  int
	deadline time.Time
}

func (f *fakeClient) PublishMsg(msg *natsgo.Msg) error {
	if f.pubErr != nil {
		return f.pubErr
	}
	f.msgs = append(f.msgs, msg)
	return nil
}

// FlushWithContext refuses a context without a deadline, as nats.go does.
func (f *fakeClient) FlushWithContext(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return natsgo.ErrNoDeadlineContext
	}
	f.flushes++
	f.deadline = deadline
	return nil
}

func TestPublishMapsMessages(t *testing.T) {
	client := &fakeClient{}
	pub := New(client)

	err := pub.Publish(context.Background(), []outbox.Message{{
		ID:           "id-1",
		Topic:        "orders.created",
		PartitionKey: "order-1",
		Payload:      []byte("payload"),
		ContentType:  "application/json",
		MessageType:  "OrderCreated",
		Headers:      map[string]string{"trace-id": "trace-1"},
	}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(client.msgs) != 1 {
		t.Fatalf("published %d messages, want 1", len(client.msgs))
	}
	msg := client.msgs[0]
	if msg.Subject != "orders.created" || string(msg.Data) != "payload" {
		t.Fatalf("message = %#v", msg)
	}
	if got := msg.Header.Get("trace-id"); got != "trace-1" {
		t.Fatalf("trace-id = %q", got)
	}
	if got := msg.Header.Get(HeaderOutboxID); got != "id-1" {
		t.Fatalf("%s = %q", HeaderOutboxID, got)
	}
	if got := msg.Header.Get(HeaderPartitionKey); got != "order-1" {
		t.Fatalf("%s = %q", HeaderPartitionKey, got)
	}
	if client.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", client.flushes)
	}
}

func TestPublishReturnsTransportError(t *testing.T) {
	want := errors.New("publish")
	err := New(&fakeClient{pubErr: want}).Publish(context.Background(), []outbox.Message{{Topic: "t"}})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

// The relay's Publish context has no deadline; the flush brings its own.
func TestPublishFlushTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opts []Option
		want time.Duration
	}{
		{name: "default as nats.go Flush", want: defaultFlushTimeout},
		{name: "configured", opts: []Option{WithFlushTimeout(time.Second)}, want: time.Second},
		{name: "non-positive keeps the default", opts: []Option{WithFlushTimeout(0)}, want: defaultFlushTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &fakeClient{}
			before := time.Now()
			if err := New(client, tc.opts...).Publish(context.Background(), []outbox.Message{{Topic: "t"}}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if left := client.deadline.Sub(before); left < tc.want || left > tc.want+time.Second {
				t.Fatalf("flush deadline in %v, want %v", left, tc.want)
			}
		})
	}
}
