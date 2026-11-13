package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/gopherex/pg-outbox/config"
)

// ruleFor is a GoneRule with refused rows kept (false) or collected after an
// hour with the default tombstone retention (true), at the real clock.
func ruleFor(collected bool) GoneRule {
	if !collected {
		return GoneRule{}
	}
	return NewGoneRule(config.Resolve(config.WithDeadRetention(time.Hour)))
}

// v7 builds a UUIDv7 created at t.
func v7(t time.Time) string {
	ms := uint64(t.UnixMilli())
	return fmt.Sprintf("%08x-%04x-7000-8000-000000000001", ms>>16, ms&0xffff)
}

func TestUUIDV7Time(t *testing.T) {
	t.Parallel()
	at := time.UnixMilli(1_760_000_000_123)
	for _, tc := range []struct {
		name string
		id   string
		ok   bool
	}{
		{name: "v7", id: v7(at), ok: true},
		{name: "v4", id: "9f3b2c1e-1d2a-4c3b-8a9f-0123456789ab"},
		{name: "not a uuid", id: "x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := uuidV7Time(tc.id)
			if ok != tc.ok || (ok && !got.Equal(at)) {
				t.Fatalf("uuidV7Time = %v, %v", got, ok)
			}
		})
	}
}

func TestGoneRule(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	collected := config.Resolve(config.WithDeadRetention(24*time.Hour), config.WithDiscardRetention(time.Hour),
		config.WithTombstoneRetention(30*24*time.Hour)) // window: 1h + 30d - 1h = 30d
	tiny := config.Resolve(config.WithDeadRetention(time.Millisecond), config.WithTombstoneRetention(time.Millisecond))
	for _, tc := range []struct {
		name      string
		set       config.Settings
		id        string
		delivered bool
	}{
		{name: "refusals kept: any gone id", set: config.Resolve(), id: "9f3b2c1e-1d2a-4c3b-8a9f-0123456789ab", delivered: true},
		{name: "recent v7", set: collected, id: v7(now.Add(-29 * 24 * time.Hour)), delivered: true},
		{name: "v7 past the horizon", set: collected, id: v7(now.Add(-31 * 24 * time.Hour))},
		{name: "v4 carries no time", set: collected, id: "9f3b2c1e-1d2a-4c3b-8a9f-0123456789ab"},
		{name: "horizon shorter than the safety margin", set: tiny, id: v7(now)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := newGoneRule(tc.set, clock).delivered(tc.id); got != tc.delivered {
				t.Fatalf("delivered(%s) = %v, want %v", tc.id, got, tc.delivered)
			}
		})
	}
}

func BenchmarkGoneRule(b *testing.B) {
	rule := ruleFor(true)
	id := v7(time.Now())
	for b.Loop() {
		_ = rule.delivered(id)
	}
}
