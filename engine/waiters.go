package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gopherex/pg-outbox/message"
	"github.com/jackc/pgx/v5/pgtype"
)

// DoneChannel is the LISTEN/NOTIFY channel on which a relay announces rows that
// reached a terminal status (published, dead, discarded). The payload is
// "<schema>:<status>:<id>,<id>,..." with at most doneChunk ids, sent inside the
// completing statement, so a listener sees it only after the commit.
const DoneChannel = "outbox_messages_done"

// doneChunk bounds the ids per notification: 200 canonical uuids (37 bytes with
// the comma) plus the prefix stay well under PostgreSQL's 8000-byte payload cap.
const doneChunk = 200

// minRecheckTimeout floors the shared recheck's deadline, so a short recheck
// interval cannot starve the query under database load.
const minRecheckTimeout = time.Second

// ErrInvalidID is returned for an id that is not a uuid.
var ErrInvalidID = errors.New("outbox: message id is not a uuid")

// ErrUndelivered matches an UndeliveredError through errors.Is.
var ErrUndelivered = errors.New("outbox: messages will not be delivered")

// UndeliveredError names rows that ended dead or discarded: they will not be
// published unless requeued, so waiting for them cannot succeed.
type UndeliveredError struct {
	IDs []string // sorted, canonical
}

func (e *UndeliveredError) Error() string {
	return fmt.Sprintf("%s: %s", ErrUndelivered, strings.Join(e.IDs, ", "))
}

// Is makes errors.Is(err, ErrUndelivered) true.
func (e *UndeliveredError) Is(target error) bool { return target == ErrUndelivered }

// CanonicalIDs validates ids as uuids and returns them deduplicated in the
// canonical lowercase hyphenated form PostgreSQL prints, so they compare equal
// to ids read back from the table or a notification.
func CanonicalIDs(ids []string) ([]string, error) {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		var u pgtype.UUID
		if err := u.Scan(id); err != nil {
			return nil, fmt.Errorf("%w: %q", ErrInvalidID, id)
		}
		c := u.String()
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out, nil
}

// donePrefix is the payload prefix for one schema and terminal status.
func donePrefix(schema string, status message.Status) string {
	return schema + ":" + string(status) + ":"
}

// parseDone splits a done payload. ok is false for a malformed payload.
func parseDone(payload string) (schema string, status message.Status, ids []string, ok bool) {
	schema, rest, found := strings.Cut(payload, ":")
	if !found {
		return "", "", nil, false
	}
	st, list, found := strings.Cut(rest, ":")
	if !found || list == "" {
		return "", "", nil, false
	}
	return schema, message.Status(st), strings.Split(list, ","), true
}

// stateReader returns the status of every listed row that exists and is not
// published; a row absent from the result is delivered.
type stateReader func(ctx context.Context, ids []string) (map[string]message.Status, error)

// Waiters resolves WaitPublished calls. A call registers its ids, then learns
// their outcome from done notifications (no query) or from a shared recheck.
// The recheck runs at most once per interval for all waiting ids together, so
// its load does not grow with the number of waiters; it covers notifications
// that never arrive (listener reconnect, no listener, LISTEN disabled).
type Waiters struct {
	schema   string
	read     stateReader
	interval time.Duration

	mu        sync.Mutex
	byID      map[string][]*waiter
	checking  bool
	lastCheck time.Time // monotonic
	lastErr   error     // last failed recheck, reported if a wait times out
}

// waiter is one WaitPublished call; guarded by Waiters.mu.
type waiter struct {
	pending     map[string]struct{}
	undelivered []string
	done        chan struct{} // closed once the outcome is known
}

// NewWaiters builds the waiter registry for one schema. interval paces the
// shared recheck.
func NewWaiters(schema string, read stateReader, interval time.Duration) *Waiters {
	return &Waiters{schema: schema, read: read, interval: interval, byID: map[string][]*waiter{}}
}

// Wait blocks until every id is delivered, one ends undelivered, or ctx ends.
// ids must be canonical (CanonicalIDs).
func (w *Waiters) Wait(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	wt := w.register(ids)
	defer w.unregister(wt, ids)

	// Check after registering: a notification racing the check still finds the
	// waiter, so nothing between the two is lost.
	states, err := w.read(ctx, ids)
	if err != nil {
		return err
	}
	w.apply(ids, states)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-wt.done:
			return w.outcome(wt)
		case <-ticker.C:
			w.recheck(ctx)
		case <-ctx.Done():
			w.mu.Lock()
			last := w.lastErr
			w.mu.Unlock()
			return errors.Join(fmt.Errorf("outbox: wait for delivery: %w", ctx.Err()), last)
		}
	}
}

// Notify applies one done notification. Notifications for another schema or
// malformed ones are ignored.
func (w *Waiters) Notify(payload string) {
	schema, status, ids, ok := parseDone(payload)
	if !ok || schema != w.schema {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range ids {
		w.settleLocked(id, status)
	}
}

func (w *Waiters) register(ids []string) *waiter {
	wt := &waiter{pending: make(map[string]struct{}, len(ids)), done: make(chan struct{})}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range ids {
		wt.pending[id] = struct{}{}
		w.byID[id] = append(w.byID[id], wt)
	}
	return wt
}

func (w *Waiters) unregister(wt *waiter, ids []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range ids {
		rest := slices.DeleteFunc(w.byID[id], func(o *waiter) bool { return o == wt })
		if len(rest) == 0 {
			delete(w.byID, id)
		} else {
			w.byID[id] = rest
		}
	}
}

func (w *Waiters) outcome(wt *waiter) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(wt.undelivered) == 0 {
		return nil
	}
	ids := slices.Clone(wt.undelivered)
	slices.Sort(ids)
	return &UndeliveredError{IDs: ids}
}

// apply records a state read: a checked id absent from states is delivered.
// A read only ever settles waiters, so a stale one cannot undo a notification.
func (w *Waiters) apply(checked []string, states map[string]message.Status) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range checked {
		st, found := states[id]
		if !found {
			st = message.StatusPublished
		}
		w.settleLocked(id, st)
	}
}

// settleLocked moves every waiter of id forward by status. Non-terminal
// statuses change nothing.
func (w *Waiters) settleLocked(id string, status message.Status) {
	switch status {
	case message.StatusPublished, message.StatusDead, message.StatusDiscarded:
	default:
		return
	}
	for _, wt := range w.byID[id] {
		if _, pending := wt.pending[id]; !pending {
			continue
		}
		delete(wt.pending, id)
		if status != message.StatusPublished {
			wt.undelivered = append(wt.undelivered, id)
		}
		if len(wt.pending) == 0 {
			close(wt.done)
		}
	}
}

// recheck reads the state of every waiting id in one query, at most once per
// interval across all waiters; concurrent callers skip while one runs. The read
// serves every waiter, so it runs under its own deadline (one interval, at
// least minRecheckTimeout) rather
// than the triggering caller's context: one caller giving up must not fail or
// cut short the read for the others. A failed read is kept for timeout reports
// and retried on the next tick.
func (w *Waiters) recheck(ctx context.Context) {
	w.mu.Lock()
	if w.checking || len(w.byID) == 0 || (!w.lastCheck.IsZero() && time.Since(w.lastCheck) < w.interval) {
		w.mu.Unlock()
		return
	}
	w.checking = true
	ids := make([]string, 0, len(w.byID))
	for id := range w.byID {
		ids = append(ids, id)
	}
	w.mu.Unlock()

	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), max(w.interval, minRecheckTimeout))
	states, err := w.read(readCtx, ids)
	cancel()

	w.mu.Lock()
	w.checking = false
	w.lastCheck = time.Now()
	w.lastErr = err
	w.mu.Unlock()
	if err == nil {
		w.apply(ids, states)
	}
}
