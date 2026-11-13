package engine

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gopherex/pg-outbox/message"
	"github.com/gopherex/pg-outbox/port"
)

// settler commits outcomes a publisher reports early (port.Settle) while the
// rest of its batch is still being published, so a finished row stops holding
// its partition. Reports queue up and are group-committed: the first report
// opens a short linger window (SettleLinger), and everything reported within
// it, or before Publish returns, commits in one statement, so a fast publisher
// costs about as many UPDATEs as a batch return.
//
// Lease renewal and early commits are serialized (io), so the heartbeat
// renews exactly the rows still in flight and never mistakes a row committed
// a moment ago for a lost lease.
type settler struct {
	relay  *Relay
	ctx    context.Context // relay context for the commits
	rec    outcomes
	claims map[string]message.Message // the batch, by id; immutable

	io sync.Mutex // serializes renew and commit (database I/O, never taken by Settle)
	// inFlight are claims not yet committed (guarded by io): renewed by the
	// heartbeat, settled by Publish's return when never reported.
	inFlight map[string]message.Message

	mu       sync.Mutex
	reported map[string]bool // ids already reported
	queue    []early
	closed   bool
	fatal    error   // first fatal report; the row stays claimed
	errs     []error // commit failures (lease lost, database)

	wake    chan struct{}
	closing chan struct{} // closed by close: commit what is queued, then stop
	done    chan struct{}
}

type early struct {
	m       message.Message
	outcome error
}

var _ port.Settler = (*settler)(nil)

func newSettler(r *Relay, ctx context.Context, msgs []message.Message, rec outcomes) *settler {
	claims := make(map[string]message.Message, len(msgs))
	inFlight := make(map[string]message.Message, len(msgs))
	for _, m := range msgs {
		claims[m.ID], inFlight[m.ID] = m, m
	}
	return &settler{
		relay: r, ctx: ctx, rec: rec, claims: claims, inFlight: inFlight,
		reported: map[string]bool{}, wake: make(chan struct{}, 1), closing: make(chan struct{}), done: make(chan struct{}),
	}
}

// Settle queues one early outcome (port.Settler).
func (s *settler) Settle(m message.Message, outcome error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closed:
		return port.ErrNotSettling
	case s.reported[m.ID]:
		return port.ErrAlreadySettled
	}
	claim, ok := s.claims[m.ID]
	if !ok {
		return port.ErrUnknownMessage
	}
	s.reported[m.ID] = true
	if outcome != nil && isFatal(s.ctx, s.relay.set, outcome) {
		// Not committed: the row stays claimed (and renewed) until the relay
		// stops after Publish returns.
		if s.fatal == nil {
			s.fatal = outcome
		}
		return nil
	}
	s.queue = append(s.queue, early{m: claim, outcome: outcome})
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

// run group-commits queued reports until close; owned by processBatch. After
// the first report it lingers (SettleLinger) so reports arriving together
// commit in one statement; closing ends the linger at once.
func (s *settler) run() {
	defer close(s.done)
	for {
		select {
		case <-s.wake:
		case <-s.closing:
		}
		linger := time.NewTimer(s.relay.set.SettleLinger)
		select {
		case <-linger.C:
		case <-s.closing:
			linger.Stop()
		}
		s.mu.Lock()
		batch := s.queue
		s.queue = nil
		closed := s.closed
		s.mu.Unlock()
		if len(batch) > 0 {
			s.commit(batch)
		}
		if closed {
			s.mu.Lock()
			empty := len(s.queue) == 0
			s.mu.Unlock()
			if empty {
				return
			}
		}
	}
}

func (s *settler) commit(batch []early) {
	msgs := make([]message.Message, len(batch))
	outcomes := make(map[string]error, len(batch))
	for i, e := range batch {
		msgs[i] = e.m
		outcomes[e.m.ID] = e.outcome
	}
	s.io.Lock()
	err := s.relay.settleEach(s.ctx, msgs, &port.BatchError{Outcomes: outcomes}, s.rec)
	for _, m := range msgs {
		delete(s.inFlight, m.ID) // committed, or no longer ours (lease lost)
	}
	s.io.Unlock()
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, err)
		s.mu.Unlock()
	}
}

// close stops taking reports and waits for the queued ones to commit.
// Idempotent.
func (s *settler) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.done
		return
	}
	s.closed = true
	s.mu.Unlock()
	close(s.closing)
	<-s.done
}

// renew extends the leases of the rows still in flight.
func (s *settler) renew(ctx context.Context, lease time.Duration) error {
	s.io.Lock()
	defer s.io.Unlock()
	if len(s.inFlight) == 0 {
		return nil
	}
	msgs := make([]message.Message, 0, len(s.inFlight))
	for _, m := range s.inFlight {
		msgs = append(msgs, m)
	}
	return s.relay.store.renew(ctx, msgs, lease)
}

// remaining returns the claims never reported, in batch order; call after close.
func (s *settler) remaining(msgs []message.Message) []message.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]message.Message, 0, len(msgs))
	for _, m := range msgs {
		if !s.reported[m.ID] {
			out = append(out, m)
		}
	}
	return out
}

// result returns the first fatal report and the commit failures; call after
// close.
func (s *settler) result() (fatal error, failed error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fatal, errors.Join(s.errs...)
}
