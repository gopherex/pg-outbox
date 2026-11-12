package integration

import (
	"context"
	"testing"
	"time"

	outbox "github.com/gopherex/pg-outbox"
)

// BenchmarkRelayThroughput measures end-to-end relay throughput: each
// iteration enqueues a batch and waits until the relay has published it.
func BenchmarkRelayThroughput(b *testing.B) {
	const rows = 1000
	pool := newTestPool(b)
	pub := &capturePublisher{}
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
		for len(pub.published()) < total {
			time.Sleep(time.Millisecond)
		}
	}
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "rows/s")
}
