package engine

import (
	"context"
	"sync"
	"time"

	"github.com/gopherex/pg-outbox/config"
	"github.com/gopherex/pg-outbox/port"
)

// Relay claims ready rows, publishes them, and records the outcome.
type Relay struct {
	store Store
	pub   port.Publisher
	set   config.Settings
}

// NewRelay builds a Relay.
func NewRelay(s Store, pub port.Publisher, set config.Settings) *Relay {
	return &Relay{store: s, pub: pub, set: set}
}

// processBatch claims one batch, publishes it, and records the outcome. It
// returns the number of rows claimed (used by drain to decide whether to
// continue).
func (r *Relay) processBatch(ctx context.Context) (int, error) {
	msgs, err := r.store.claim(ctx, r.set)
	if err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	if perr := r.pub.Publish(ctx, msgs); perr != nil {
		for _, m := range msgs {
			max := r.set.MaxAttempts
			if m.MaxAttempts != nil {
				max = *m.MaxAttempts
			}
			if m.Attempts >= max {
				if err := r.store.markDead(ctx, m.ID, perr.Error()); err != nil {
					return len(msgs), err
				}
				r.set.Hooks.OnDead(ctx, m)
			} else {
				if err := r.store.markRetry(ctx, m.ID, perr.Error(), r.set.Backoff(m.Attempts)); err != nil {
					return len(msgs), err
				}
				r.set.Hooks.OnFailed(ctx, m, perr)
			}
		}
		return len(msgs), nil
	}

	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	if err := r.store.markPublished(ctx, ids); err != nil {
		return len(msgs), err
	}
	r.set.Hooks.OnPublished(ctx, msgs)
	return len(msgs), nil
}

// drain processes batches until fewer than batchSize rows are claimed (i.e. the
// ready queue is empty for now).
func (r *Relay) drain(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := r.processBatch(ctx)
		if err != nil {
			r.set.Logger.Error("outbox: relay batch failed", "err", err)
			return
		}
		if n < r.set.BatchSize {
			return
		}
	}
}

// Run starts the relay: set.Concurrency worker goroutines plus an optional
// LISTEN/NOTIFY listener. It blocks until ctx is cancelled, then drains in
// flight work and returns.
func (r *Relay) Run(ctx context.Context) error {
	wake := make(chan struct{}, 1)
	var wg sync.WaitGroup

	// LISTEN/NOTIFY needs a dedicated pooled connection. Without a pool we run
	// poll-only.
	if r.set.Notify && r.store.pool != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.listen(ctx, wake)
		}()
	} else if r.set.Notify {
		r.set.Logger.Warn("outbox: no listen pool configured, relay runs poll-only")
	}

	for i := 0; i < r.set.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.worker(ctx, wake)
		}()
	}
	wg.Wait()
	return nil
}

func (r *Relay) worker(ctx context.Context, wake <-chan struct{}) {
	ticker := time.NewTicker(r.set.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
		r.drain(ctx)
	}
}

// listen holds a dedicated connection on LISTEN and signals wake on every
// notification. It reconnects on error until ctx is cancelled.
func (r *Relay) listen(ctx context.Context, wake chan<- struct{}) {
	for ctx.Err() == nil {
		if err := r.listenOnce(ctx, wake); err != nil && ctx.Err() == nil {
			r.set.Logger.Warn("outbox: listen connection lost, retrying", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

func (r *Relay) listenOnce(ctx context.Context, wake chan<- struct{}) error {
	conn, err := r.store.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return err
	}
	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return err
		}
		select {
		case wake <- struct{}{}:
		default: // a wake is already pending; coalesce
		}
	}
}
