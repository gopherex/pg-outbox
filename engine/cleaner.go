package engine

import (
	"context"
	"time"

	"github.com/gopherex/pg-outbox/config"
)

// Cleaner deletes terminal rows according to their opt-in retention windows.
type Cleaner struct {
	store Store
	set   config.Settings
}

// NewCleaner builds a Cleaner.
func NewCleaner(s Store, set config.Settings) *Cleaner {
	return &Cleaner{store: s, set: set}
}

// Run is a no-op unless at least one retention window and the cleanup
// interval are set. Otherwise it blocks until ctx is cancelled and returns nil,
// or returns the first fatal error (see isFatal), including a recovered panic.
func (c *Cleaner) Run(ctx context.Context) error {
	return recovered(func() error { return c.run(ctx) })
}

func (c *Cleaner) run(ctx context.Context) error {
	if c.set.CleanupInterval <= 0 || (c.set.Retention <= 0 && c.set.DeadRetention <= 0 && c.set.DiscardRetention <= 0) {
		return nil
	}
	ticker := time.NewTicker(c.set.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		for ctx.Err() == nil {
			n, err := c.store.cleanup(ctx, c.set)
			if err != nil {
				if isFatal(ctx, c.set, err) {
					return err
				}
				c.set.Logger.Error("outbox: cleanup failed", "err", err)
				break
			}
			if n > 0 {
				c.set.Hooks.OnCleanup(ctx, int(n))
			}
			if int(n) < c.set.BatchSize {
				break
			}
		}
	}
}
