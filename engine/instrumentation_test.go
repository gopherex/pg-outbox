package engine

import (
	"errors"
	"testing"

	"github.com/gopherex/pg-outbox/message"
	"github.com/gopherex/pg-outbox/port"
)

func TestEndDeliveries(t *testing.T) {
	t.Parallel()
	errAbort := errors.New("abort")
	msgs := []message.Message{{ID: "published"}, {ID: "claimed"}, {ID: "unopened"}}
	rec := outcomes{"published": {Status: message.StatusPublished}}
	for _, tc := range []struct {
		name    string
		err     error
		wantErr error
	}{
		{name: "fatal error is unwrapped", err: fatalError{errAbort}, wantErr: errAbort},
		{name: "plain error", err: port.ErrLeaseLost, wantErr: port.ErrLeaseLost},
		{name: "panic leaves no error", err: nil, wantErr: errDeliveryInterrupted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := map[string][]port.Outcome{}
			ends := map[string]func(port.Outcome){}
			for _, id := range []string{"published", "claimed"} {
				ends[id] = func(o port.Outcome) { got[id] = append(got[id], o) }
			}
			endDeliveries(msgs, ends, rec, tc.err)
			if o := got["published"]; len(o) != 1 || o[0].Status != message.StatusPublished || o[0].Err != nil {
				t.Fatalf("published: %v", o)
			}
			if o := got["claimed"]; len(o) != 1 || o[0].Status != message.StatusProcessing || !errors.Is(o[0].Err, tc.wantErr) {
				t.Fatalf("claimed: %v, want processing with %v", o, tc.wantErr)
			}
			if _, ended := got["unopened"]; ended {
				t.Fatal("a delivery never opened was ended")
			}
		})
	}
}
