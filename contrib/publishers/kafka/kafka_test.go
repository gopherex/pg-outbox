package kafka

import (
	"context"
	"errors"
	"testing"

	outbox "github.com/gopherex/pg-outbox"
	kafkago "github.com/segmentio/kafka-go"
)

type fakeWriter struct {
	msgs []kafkago.Message
	err  error
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}

func TestPublishMapsMessages(t *testing.T) {
	writer := &fakeWriter{}
	pub := New(writer)

	err := pub.Publish(context.Background(), []outbox.Message{{
		ID:           "id-1",
		Topic:        "orders",
		PartitionKey: "order-1",
		Payload:      []byte("payload"),
		ContentType:  "application/json",
		MessageType:  "OrderCreated",
		Headers:      map[string]string{"trace-id": "trace-1"},
	}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(writer.msgs) != 1 {
		t.Fatalf("published %d messages, want 1", len(writer.msgs))
	}
	msg := writer.msgs[0]
	if msg.Topic != "orders" || string(msg.Key) != "order-1" || string(msg.Value) != "payload" {
		t.Fatalf("message = %#v", msg)
	}
	assertHeader(t, msg.Headers, "trace-id", "trace-1")
	assertHeader(t, msg.Headers, HeaderOutboxID, "id-1")
	assertHeader(t, msg.Headers, HeaderContentType, "application/json")
	assertHeader(t, msg.Headers, HeaderMessageType, "OrderCreated")
}

func TestPublishReturnsWriterError(t *testing.T) {
	want := errors.New("write")
	err := New(&fakeWriter{err: want}).Publish(context.Background(), []outbox.Message{{Topic: "t"}})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func assertHeader(t *testing.T, headers []kafkago.Header, key, want string) {
	t.Helper()
	for _, h := range headers {
		if h.Key == key {
			if string(h.Value) != want {
				t.Fatalf("%s = %q, want %q", key, h.Value, want)
			}
			return
		}
	}
	t.Fatalf("missing header %s", key)
}
