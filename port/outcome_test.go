package port

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestOutcomeWrapping(t *testing.T) {
	cause := errors.New("transport failure")
	for _, err := range []error{Permanent(cause), RetryAfter(cause, time.Minute), Discard(cause), Abort(cause), &PanicError{Value: cause}} {
		wrapped := fmt.Errorf("adapter: %w", err)
		if !errors.Is(wrapped, cause) {
			t.Fatalf("lost cause: %v", wrapped)
		}
	}
	var permanent *PermanentError
	if !errors.As(fmt.Errorf("wrapped: %w", Permanent(cause)), &permanent) {
		t.Fatal("missing permanent marker")
	}
	var retry *RetryAfterError
	if !errors.As(RetryAfter(cause, time.Minute), &retry) || retry.Delay != time.Minute {
		t.Fatal("missing retry delay")
	}
	var discard *DiscardError
	if !errors.As(Discard(cause), &discard) {
		t.Fatal("missing discard marker")
	}
	var abort *AbortError
	if !errors.As(fmt.Errorf("wrapped: %w", Abort(cause)), &abort) {
		t.Fatal("missing abort marker")
	}
	if got := (&PanicError{Value: 42}).Error(); got != "outbox: panic: 42" {
		t.Fatalf("panic text = %q", got)
	}
	if (&PanicError{Value: "text"}).Unwrap() != nil {
		t.Fatal("non-error panic value must not unwrap")
	}
	for _, err := range []error{Permanent(nil), RetryAfter(nil, 0), Discard(nil), Abort(nil)} {
		if err == nil || err.Error() == "" {
			t.Fatal("nil cause must still express an outcome")
		}
	}
}
