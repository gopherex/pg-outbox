// Package backoff provides retry backoff strategies for the outbox relay.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Backoff returns the delay before the retry that follows the given attempt
// number. attempt is 1-based: the delay after the first failed attempt is
// Backoff(1).
type Backoff func(attempt int) time.Duration

// Exp returns an exponential backoff: base doubled each attempt, capped at
// maxDelay (0 = no cap). With jitter the result is uniform in [0, computed].
func Exp(base, maxDelay time.Duration, jitter bool) Backoff {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		shift := attempt - 1
		if shift > 62 { // guard against overflow of int64 shift
			shift = 62
		}
		d := base << uint(shift)
		if d < base { // overflowed
			d = maxDelay
		}
		if maxDelay > 0 && d > maxDelay {
			d = maxDelay
		}
		if jitter && d > 0 {
			d = time.Duration(rand.Int64N(int64(d) + 1))
		}
		return d
	}
}

// Default is exponential 100ms -> 30s with full jitter.
var Default = Exp(100*time.Millisecond, 30*time.Second, true)

// Delay lets a Backoff also satisfy interfaces that expect a Delay(attempt)
// method, and reads naturally at call sites.
func (b Backoff) Delay(attempt int) time.Duration { return b(attempt) }

// ---- Adapters for popular backoff libraries ---------------------------
//
// The outbox model is stateless and attempt-indexed: a message's next delay is
// recomputed from its persisted attempt count, even across process restarts. So
// a single shared stateful backoff instance cannot be used directly (it would
// also be a data race across relay goroutines). These adapters take a *factory*
// that yields a fresh stateful backoff and advance it `attempt` times. They use
// locally-declared structural interfaces, so this module depends on none of
// those libraries — pass an instance and Go's structural typing does the rest.

// Stop is the sentinel a NextBackOff-style source returns to mean "no more
// retries"; it matches github.com/cenkalti/backoff.Stop. Adapters map it to 0.
const Stop = time.Duration(-1)

// NextBackOffer matches the retry method of github.com/cenkalti/backoff
// (BackOff.NextBackOff). *backoff.ExponentialBackOff and friends satisfy it.
type NextBackOffer interface {
	NextBackOff() time.Duration
}

// FromNextBackOff adapts a cenkalti/backoff-style source to a Backoff. newB must
// return a fresh instance on each call; it is advanced `attempt` times to obtain
// that attempt's delay. A Stop result yields 0.
//
//	ob.WithRetryBackoff(backoff.FromNextBackOff(func() backoff.NextBackOffer {
//	    return cbackoff.NewExponentialBackOff()
//	}))
func FromNextBackOff(newB func() NextBackOffer) Backoff {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		b := newB()
		var d time.Duration
		for i := 0; i < attempt; i++ {
			d = b.NextBackOff()
			if d == Stop {
				return 0
			}
		}
		return d
	}
}

// Nexter matches the Backoff interface of github.com/sethvargo/go-retry
// (Next() (time.Duration, bool)).
type Nexter interface {
	Next() (time.Duration, bool)
}

// FromNexter adapts a sethvargo/go-retry-style source to a Backoff. newB must
// return a fresh instance on each call; it is advanced `attempt` times. A stop
// signal yields 0.
func FromNexter(newB func() Nexter) Backoff {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		b := newB()
		var d time.Duration
		for i := 0; i < attempt; i++ {
			var stop bool
			d, stop = b.Next()
			if stop {
				return 0
			}
		}
		return d
	}
}
