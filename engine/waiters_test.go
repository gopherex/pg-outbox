package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gopherex/pg-outbox/message"
)

const (
	idA = "0190a5f0-0000-7000-8000-00000000000a"
	idB = "0190a5f0-0000-7000-8000-00000000000b"
)

func TestCanonicalIDs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		in   []string
		want []string
		err  error
	}{
		{name: "canonical", in: []string{idA}, want: []string{idA}},
		{name: "upper case", in: []string{"0190A5F0-0000-7000-8000-00000000000A"}, want: []string{idA}},
		{name: "no hyphens", in: []string{"0190a5f000007000800000000000000a"}, want: []string{idA}},
		{name: "duplicates", in: []string{idA, idB, "0190A5F0-0000-7000-8000-00000000000A"}, want: []string{idA, idB}},
		{name: "empty", in: nil, want: []string{}},
		{name: "not a uuid", in: []string{idA, "order-42"}, err: ErrInvalidID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := CanonicalIDs(tc.in)
			if !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want %v", err, tc.err)
			}
			if tc.err == nil && !slices.Equal(got, tc.want) {
				t.Fatalf("ids = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseDone(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		payload string
		schema  string
		status  message.Status
		ids     []string
		ok      bool
	}{
		{payload: "public:published:" + idA + "," + idB, schema: "public", status: message.StatusPublished, ids: []string{idA, idB}, ok: true},
		{payload: donePrefix("events", message.StatusDead) + idA, schema: "events", status: message.StatusDead, ids: []string{idA}, ok: true},
		{payload: "public:published:"},
		{payload: "public"},
		{payload: ""},
	} {
		t.Run(tc.payload, func(t *testing.T) {
			t.Parallel()
			schema, status, ids, ok := parseDone(tc.payload)
			if ok != tc.ok || schema != tc.schema || status != tc.status || !slices.Equal(ids, tc.ids) {
				t.Fatalf("parseDone = %q %q %v %v", schema, status, ids, ok)
			}
		})
	}
}

// fakeStates serves a mutable row-state table and counts reads.
type fakeStates struct {
	mu     sync.Mutex
	states map[string]message.Status
	err    error
	reads  atomic.Int32
}

func (f *fakeStates) set(id string, st message.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[id] = st
}

func (f *fakeStates) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeStates) read(_ context.Context, ids []string) (map[string]message.Status, error) {
	f.reads.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]message.Status{}
	for _, id := range ids {
		if st, ok := f.states[id]; ok {
			out[id] = st
		}
	}
	return out, nil
}

func newFake(pending ...string) *fakeStates {
	f := &fakeStates{states: map[string]message.Status{}}
	for _, id := range pending {
		f.states[id] = message.StatusPending
	}
	return f
}

// waitAsync starts Wait and returns its result channel once the waiter is
// registered (its first read happened).
func waitAsync(t *testing.T, w *Waiters, f *fakeStates, ctx context.Context, ids ...string) <-chan error {
	t.Helper()
	before := f.reads.Load()
	res := make(chan error, 1)
	go func() { res <- w.Wait(ctx, ids) }()
	deadline := time.Now().Add(2 * time.Second)
	for f.reads.Load() == before {
		if time.Now().After(deadline) {
			t.Fatal("waiter never registered")
		}
		time.Sleep(time.Millisecond)
	}
	return res
}

func result(t *testing.T, res <-chan error) error {
	t.Helper()
	select {
	case err := <-res:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not finish")
		return nil
	}
}

func TestWaitersOutcomes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		pending []string         // rows pending at the first read
		notify  []string         // payloads delivered after it
		want    func(error) bool // outcome
		undeliv []string         // ids in the UndeliveredError
	}{
		{name: "absent rows are delivered", want: func(err error) bool { return err == nil }},
		{name: "published by notification", pending: []string{idA, idB},
			notify: []string{"public:published:" + idA, "public:published:" + idB},
			want:   func(err error) bool { return err == nil }},
		{name: "one chunk settles many", pending: []string{idA, idB},
			notify: []string{"public:published:" + idA + "," + idB},
			want:   func(err error) bool { return err == nil }},
		{name: "dead and discarded are undelivered", pending: []string{idA, idB},
			notify:  []string{"public:discarded:" + idB, "public:dead:" + idA},
			want:    func(err error) bool { return errors.Is(err, ErrUndelivered) },
			undeliv: []string{idA, idB}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFake(tc.pending...)
			w := NewWaiters("public", f.read, time.Hour, false)
			res := waitAsync(t, w, f, t.Context(), idA, idB)
			for _, p := range tc.notify {
				w.Notify(p)
			}
			err := result(t, res)
			if !tc.want(err) {
				t.Fatalf("outcome = %v", err)
			}
			var u *UndeliveredError
			if errors.As(err, &u) && !slices.Equal(u.IDs, tc.undeliv) {
				t.Fatalf("undelivered = %v, want %v", u.IDs, tc.undeliv)
			}
			if n := len(w.byID); n != 0 {
				t.Fatalf("%d ids still registered after the wait", n)
			}
		})
	}
}

// Notifications that do not concern the waiter change nothing.
func TestWaitersIgnoreForeignNotifications(t *testing.T) {
	t.Parallel()
	f := newFake(idA)
	w := NewWaiters("public", f.read, time.Hour, false)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	res := waitAsync(t, w, f, ctx, idA)
	for _, p := range []string{
		"events:published:" + idA, // another schema with the same id
		"public:published:" + idB, // another row
		"public:pending:" + idA,   // not terminal
		"garbage",
	} {
		w.Notify(p)
	}
	if err := result(t, res); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("outcome = %v, want the deadline", err)
	}
}

// Without notifications, the shared recheck settles every waiter, and many
// waiters still cost about one read per interval.
func TestWaitersRecheckIsShared(t *testing.T) {
	t.Parallel()
	const waiters = 50
	interval := 20 * time.Millisecond
	f := newFake(idA)
	w := NewWaiters("public", f.read, interval, false)
	results := make([]<-chan error, waiters)
	for i := range results {
		results[i] = waitAsync(t, w, f, t.Context(), idA)
	}
	initial := f.reads.Load()
	time.Sleep(10 * interval)
	if rechecks := f.reads.Load() - initial; rechecks > 15 {
		t.Fatalf("%d rechecks in 10 intervals for %d waiters: not shared", rechecks, waiters)
	}
	f.set(idA, message.StatusPublished)
	for _, res := range results {
		if err := result(t, res); err != nil {
			t.Fatal(err)
		}
	}
}

// A wait that times out reports the last recheck failure next to the deadline.
func TestWaitersTimeoutReportsReadFailure(t *testing.T) {
	t.Parallel()
	errDB := errors.New("db down")
	f := newFake(idA)
	w := NewWaiters("public", f.read, 10*time.Millisecond, false)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	res := waitAsync(t, w, f, ctx, idA)
	f.fail(errDB)
	err := result(t, res)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errDB) {
		t.Fatalf("outcome = %v, want the deadline and the read failure", err)
	}
}

// A failing first read is returned at once.
func TestWaitersFirstReadFailure(t *testing.T) {
	t.Parallel()
	errDB := errors.New("db down")
	f := newFake()
	f.fail(errDB)
	w := NewWaiters("public", f.read, time.Hour, false)
	if err := w.Wait(t.Context(), []string{idA}); !errors.Is(err, errDB) {
		t.Fatalf("outcome = %v, want %v", err, errDB)
	}
	if n := len(w.byID); n != 0 {
		t.Fatalf("%d ids still registered", n)
	}
}

func TestVerdict(t *testing.T) {
	t.Parallel()
	pub, pend, dead := message.StatusPublished, message.StatusPending, message.StatusDead
	for _, tc := range []struct {
		name      string
		states    map[string]message.Status
		collected bool
		done      bool
		err       error
		ids       []string
	}{
		{name: "published", states: map[string]message.Status{idA: pub, idB: pub}, done: true},
		{name: "gone is delivered while refusals are kept", states: map[string]message.Status{idA: pub}, done: true},
		{name: "gone is unknown while refusals are collected", states: map[string]message.Status{idA: pub},
			collected: true, err: ErrOutcomeUnknown, ids: []string{idB}},
		{name: "in flight", states: map[string]message.Status{idA: pub, idB: pend}},
		{name: "refusal wins over unknown", states: map[string]message.Status{idA: dead}, collected: true,
			err: ErrUndelivered, ids: []string{idA}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			done, err := Verdict([]string{idA, idB}, tc.states, tc.collected)
			if done != tc.done || !errors.Is(err, tc.err) || (tc.err == nil) != (err == nil) {
				t.Fatalf("Verdict = %v, %v; want %v, %v", done, err, tc.done, tc.err)
			}
			var u *UndeliveredError
			var k *UnknownOutcomeError
			switch {
			case errors.As(err, &u) && !slices.Equal(u.IDs, tc.ids):
				t.Fatalf("undelivered %v, want %v", u.IDs, tc.ids)
			case errors.As(err, &k) && !slices.Equal(k.IDs, tc.ids):
				t.Fatalf("unknown %v, want %v", k.IDs, tc.ids)
			}
		})
	}
}

// With refused rows collected, a wait on a gone row ends with the unknown
// outcome instead of a false success.
func TestWaitersGoneRowOutcomeUnknown(t *testing.T) {
	t.Parallel()
	w := NewWaiters("public", newFake().read, time.Hour, true)
	err := w.Wait(t.Context(), []string{idA})
	var k *UnknownOutcomeError
	if !errors.As(err, &k) || !slices.Equal(k.IDs, []string{idA}) {
		t.Fatalf("outcome = %v, want unknown for %s", err, idA)
	}
}

func BenchmarkWaitersNotify(b *testing.B) {
	ids := make([]string, doneChunk)
	for i := range ids {
		ids[i] = fmt.Sprintf("0190a5f0-0000-7000-8000-%012d", i)
	}
	w := NewWaiters("public", newFake().read, time.Hour, false)
	for _, id := range ids[:10] {
		w.register([]string{id})
	}
	payload := donePrefix("public", message.StatusPublished) + strings.Join(ids, ",")
	for b.Loop() {
		w.Notify(payload)
	}
}

func BenchmarkCanonicalIDs(b *testing.B) {
	ids := []string{idA, idB, "0190A5F0-0000-7000-8000-00000000000C"}
	for b.Loop() {
		_, _ = CanonicalIDs(ids)
	}
}
