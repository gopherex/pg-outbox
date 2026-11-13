package integration

import (
	"context"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
)

// settlingPublisher reports every message early through Settle, the way a
// publisher awaiting a per-message ack (JetStream) does.
type settlingPublisher struct{ capturePublisher }

func (p *settlingPublisher) Publish(ctx context.Context, msgs []outbox.Message) error {
	for _, m := range msgs {
		if err := outbox.Settle(ctx, m, nil); err != nil {
			return err
		}
	}
	return p.capturePublisher.Publish(ctx, msgs)
}

// BenchmarkRelayThroughput measures end-to-end relay throughput: each
// iteration enqueues a batch and waits until the relay has published it.
func BenchmarkRelayThroughput(b *testing.B) {
	b.Run("batch return", func(b *testing.B) {
		relayThroughput(b, &capturePublisher{}, func(p outbox.Publisher) int {
			return len(p.(*capturePublisher).published())
		})
	})
	b.Run("settle each", func(b *testing.B) {
		relayThroughput(b, &settlingPublisher{}, func(p outbox.Publisher) int {
			return len(p.(*settlingPublisher).published())
		})
	})
}

func relayThroughput(b *testing.B, pub outbox.Publisher, published func(outbox.Publisher) int) {
	const rows = 1000
	pool := newTestPool(b)
	ob, err := outbox.New(pool, pool, pub, outbox.WithBatchSize(100), outbox.WithPollInterval(time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ob.Run(ctx) }()

	msgs := make([]outbox.Message, rows)
	for i := range msgs {
		msgs[i] = outbox.Message{Topic: "t", Payload: []byte("x")}
	}
	total := 0
	for b.Loop() {
		if err := ob.EnqueueBatch(ctx, msgs); err != nil {
			b.Fatal(err)
		}
		total += rows
		for published(pub) < total {
			time.Sleep(time.Millisecond)
		}
	}
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "rows/s")
}
