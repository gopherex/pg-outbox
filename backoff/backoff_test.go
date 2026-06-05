package backoff

import (
	"testing"
	"time"
)

func TestExpNoJitter(t *testing.T) {
	b := Exp(100*time.Millisecond, 30*time.Second, false)
	cases := map[int]time.Duration{
		0:  100 * time.Millisecond, // attempt < 1 clamps to 1
		1:  100 * time.Millisecond,
		2:  200 * time.Millisecond,
		3:  400 * time.Millisecond,
		10: 30 * time.Second, // capped
	}
	for attempt, want := range cases {
		if got := b(attempt); got != want {
			t.Errorf("b(%d) = %v, want %v", attempt, got, want)
		}
	}
}

// fakeNextBackOff is a cenkalti/backoff-style stateful source: 1s, 2s, 3s, …
// then Stop after stopAt calls.
type fakeNextBackOff struct {
	n      int
	stopAt int
}

func (f *fakeNextBackOff) NextBackOff() time.Duration {
	f.n++
	if f.stopAt > 0 && f.n > f.stopAt {
		return Stop
	}
	return time.Duration(f.n) * time.Second
}

func TestFromNextBackOff(t *testing.T) {
	b := FromNextBackOff(func() NextBackOffer { return &fakeNextBackOff{} })
	// attempt N advances a fresh instance N times -> N seconds.
	for attempt, want := range map[int]time.Duration{1: time.Second, 3: 3 * time.Second, 5: 5 * time.Second} {
		if got := b(attempt); got != want {
			t.Errorf("b(%d) = %v, want %v", attempt, got, want)
		}
	}
	// attempt < 1 clamps to 1.
	if got := b(0); got != time.Second {
		t.Errorf("b(0) = %v, want 1s", got)
	}
	// Stop -> 0.
	bs := FromNextBackOff(func() NextBackOffer { return &fakeNextBackOff{stopAt: 2} })
	if got := bs(5); got != 0 {
		t.Errorf("stopped backoff = %v, want 0", got)
	}
}

// fakeNexter is a sethvargo/go-retry-style source: 1s, 2s, … then stop.
type fakeNexter struct {
	n      int
	stopAt int
}

func (f *fakeNexter) Next() (time.Duration, bool) {
	f.n++
	if f.stopAt > 0 && f.n > f.stopAt {
		return 0, true
	}
	return time.Duration(f.n) * time.Second, false
}

func TestFromNexter(t *testing.T) {
	b := FromNexter(func() Nexter { return &fakeNexter{} })
	if got := b(3); got != 3*time.Second {
		t.Errorf("b(3) = %v, want 3s", got)
	}
	bs := FromNexter(func() Nexter { return &fakeNexter{stopAt: 1} })
	if got := bs(4); got != 0 {
		t.Errorf("stopped backoff = %v, want 0", got)
	}
}

func TestExpJitterWithinBounds(t *testing.T) {
	b := Exp(100*time.Millisecond, 30*time.Second, true)
	for i := 0; i < 1000; i++ {
		d := b(3) // base computed = 400ms
		if d < 0 || d > 400*time.Millisecond {
			t.Fatalf("jittered delay %v out of [0,400ms]", d)
		}
	}
}
