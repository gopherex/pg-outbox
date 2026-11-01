package port

import (
	"errors"
	"fmt"
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

// AbortError stops the relay: Run returns it and the claimed batch is left
// untouched under its lease, so another instance or a restart re-claims it
// after expiry. It takes precedence over every other outcome.
// Use Abort to construct one. It supports errors.Is and errors.As.
type AbortError struct{ Err error }

func (e *AbortError) Error() string {
	if e.Err == nil {
		return "outbox: relay aborted"
	}
	return e.Err.Error()
}
func (e *AbortError) Unwrap() error { return e.Err }

// Abort stops the relay without completing, retrying or dead-lettering the batch.
func Abort(err error) error { return &AbortError{Err: err} }

// PanicError is a panic recovered inside the relay (publisher, hooks, executor).
// It is always fatal: Run returns it like an AbortError.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("outbox: panic: %v", e.Value) }

// Unwrap exposes a panicked error value to errors.Is and errors.As.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// ErrLeaseLost means the claim expired or no longer belongs to this attempt.
// It does not say whether the external side effect occurred.
var ErrLeaseLost = errors.New("outbox: lease lost")
