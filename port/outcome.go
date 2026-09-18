package port

import (
	"errors"
	"time"
)

// PermanentError stops retries and moves the batch to the dead state.
// Use Permanent to construct one. It supports errors.Is and errors.As.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string {
	if e.Err == nil {
		return "outbox: permanent publish failure"
	}
	return e.Err.Error()
}
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent marks a batch as terminally failed without further retries.
func Permanent(err error) error { return &PermanentError{Err: err} }

// RetryAfterError requests a minimum delay before another attempt. The relay
// uses the greater of Delay and its configured backoff, subject to MaxAttempts.
type RetryAfterError struct {
	Err   error
	Delay time.Duration
}

func (e *RetryAfterError) Error() string {
	if e.Err == nil {
		return "outbox: retry requested"
	}
	return e.Err.Error()
}
func (e *RetryAfterError) Unwrap() error { return e.Err }

// RetryAfter requests a minimum retry delay for the batch.
func RetryAfter(err error, delay time.Duration) error {
	return &RetryAfterError{Err: err, Delay: delay}
}

// DiscardError completes the batch without publishing or dead-lettering it.
// The application, not the outbox, decides when this is appropriate.
type DiscardError struct{ Err error }

func (e *DiscardError) Error() string {
	if e.Err == nil {
		return "outbox: discarded"
	}
	return e.Err.Error()
}
func (e *DiscardError) Unwrap() error { return e.Err }

// Discard completes a batch without publication or dead-lettering.
func Discard(reason error) error { return &DiscardError{Err: reason} }

// ErrLeaseLost means the claim expired or no longer belongs to this attempt.
// It does not say whether the external side effect occurred.
var ErrLeaseLost = errors.New("outbox: lease lost")
