package engine

import "testing"

func TestNewStoreQualifiesTable(t *testing.T) {
	s := NewStore(nil, nil, nil, "events")
	if s.Table() != `"events".outbox_messages` {
		t.Fatalf("table = %q, want \"events\".outbox_messages", s.Table())
	}
}
