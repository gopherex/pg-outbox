package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gopherex/pg-outbox/config"
	"github.com/gopherex/pg-outbox/message"
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

	perr, leaseErr := r.publish(ctx, msgs)
	if leaseErr != nil {
		return len(msgs), leaseErr
	}
	if perr == nil {
		return len(msgs), r.complete(ctx, msgs, message.StatusPublished, nil, 0)
	}
	var permanent *port.PermanentError
	var discarded *port.DiscardError
	var retry *port.RetryAfterError
	switch {
	case errors.As(perr, &permanent):
		return len(msgs), r.complete(ctx, msgs, message.StatusDead, perr, 0)
	case errors.As(perr, &discarded):
		return len(msgs), r.complete(ctx, msgs, message.StatusDiscarded, perr, 0)
	}
	errors.As(perr, &retry)
	// Attempt counts and per-message limits may differ inside one failed batch.
	// Continue finalizing other rows even if one completion fails.
	var errs []error
	for _, m := range msgs {
		max := r.set.MaxAttempts
		if m.MaxAttempts != nil {
			max = *m.MaxAttempts
		}
		status := message.StatusPending
		delay := time.Duration(0)
		if m.Attempts >= max {
			status = message.StatusDead
		} else {
			delay = r.set.Backoff(m.Attempts)
			if delay < 0 {
				delay = 0
			}
			if retry != nil && retry.Delay > delay {
				delay = retry.Delay
			}
		}
		if err := r.complete(ctx, []message.Message{m}, status, perr, delay); err != nil {
			errs = append(errs, err)
		}
	}
	return len(msgs), errors.Join(errs...)
}

func (r *Relay) complete(ctx context.Context, msgs []message.Message, status message.Status, cause error, delay time.Duration) error {
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	completed, err := r.store.finish(ctx, msgs, status, reason, delay, r.set.ClearPayloadOnComplete)
	if err != nil {
		return err
	}
	if len(completed) > 0 {
		switch status {
		case message.StatusPublished:
			r.set.Hooks.OnPublished(ctx, completed)
		case message.StatusDiscarded:
			if h, ok := r.set.Hooks.(port.DiscardHooks); ok {
				h.OnDiscarded(ctx, completed, cause)
			}
		case message.StatusDead:
			for _, m := range completed {
				r.set.Hooks.OnDead(ctx, m)
			}
		case message.StatusPending:
			for _, m := range completed {
				r.set.Hooks.OnFailed(ctx, m, cause)
			}
		}
	}
	if len(completed) != len(msgs) {
		return port.ErrLeaseLost
	}
	return nil
}

// publish renews claims while the adapter runs. Renewal failures cancel the
// publisher context; they are not converted to transport failures. The lease
// then expires for recovery. An adapter ignoring cancellation cannot be stopped
// safely by the library, but its stale result cannot change a newer claim.
func (r *Relay) publish(ctx context.Context, msgs []message.Message) (error, error) {
	pubCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if r.set.PublishTimeout > 0 {
		var timeoutCancel context.CancelFunc
		pubCtx, timeoutCancel = context.WithTimeout(pubCtx, r.set.PublishTimeout)
		defer timeoutCancel()
	}
	interval := r.set.LeaseDuration / 3
	if interval <= 0 {
		interval = time.Nanosecond
	}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				done <- nil
				return
			case <-pubCtx.Done():
				done <- nil
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(pubCtx, interval)
				err := r.store.renew(renewCtx, msgs, r.set.LeaseDuration)
				renewCancel()
				if err != nil {
					// Cancellation of the call itself is not a lease renewal failure.
					if pubCtx.Err() != nil {
						done <- nil
						return
					}
					cancel()
					done <- fmt.Errorf("outbox: renew claims: %w", err)
					return
				}
			}
		}
	}()
	// Keep claim identity private: adapters may modify the slice they receive,
	// while the heartbeat and completion must use the original claim tokens.
	perr := r.pub.Publish(pubCtx, append([]message.Message(nil), msgs...))
	close(stop)
	return perr, <-done
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
