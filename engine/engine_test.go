package engine

import (
	"context"
	"testing"

	"github.com/gopherex/pg-outbox/config"
	"github.com/gopherex/pg-outbox/message"
)

func TestNewStoreQualifiesTable(t *testing.T) {
	s := NewStore(nil, nil, nil, "events")
	if s.Table() != `"events".outbox_messages` {
		t.Fatalf("table = %q, want \"events\".outbox_messages", s.Table())
	}
}

type mutatePublisher struct{}

func (mutatePublisher) Publish(_ context.Context, msgs []message.Message) error {
	msgs[0].ID = "adapter-id"
	msgs[0].LeaseToken = "adapter-token"
	msgs[0].Attempts = 999
	return nil
}

func TestPublisherCannotChangeClaimIdentity(t *testing.T) {
	r := NewRelay(NewStore(nil, nil, nil, "public"), mutatePublisher{}, config.Resolve())
	msgs := []message.Message{{ID: "claim-id", LeaseToken: "claim-token", Attempts: 1}}
	if err, leaseErr := r.publish(context.Background(), msgs); err != nil || leaseErr != nil {
		t.Fatalf("publish: %v, %v", err, leaseErr)
	}
	if msgs[0].ID != "claim-id" || msgs[0].LeaseToken != "claim-token" || msgs[0].Attempts != 1 {
		t.Fatalf("publisher changed private claim identity: %+v", msgs[0])
	}
}
