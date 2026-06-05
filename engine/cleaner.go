package engine

import (
	"context"
	"time"

	"github.com/gopherex/pg-outbox/config"
)

// Cleaner periodically deletes published rows older than the retention window.
type Cleaner struct {
	store Store
	set   config.Settings
}

// NewCleaner builds a Cleaner.
func NewCleaner(s Store, set config.Settings) *Cleaner {
	return &Cleaner{store: s, set: set}
}

// Run is a no-op (returns immediately) unless both retention and cleanup
// interval are set. Otherwise it blocks until ctx is cancelled.
func (c *Cleaner) Run(ctx context.Context) error {
	if c.set.Retention <= 0 || c.set.CleanupInterval <= 0 {
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
			n, err := c.store.cleanup(ctx, c.set.Retention, c.set.BatchSize)
			if err != nil {
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
