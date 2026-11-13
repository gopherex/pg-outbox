package config

import (
	"errors"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	s := Default()
	if s.Schema != "public" {
		t.Errorf("schema = %q, want public", s.Schema)
	}
	if s.PollInterval != time.Second {
		t.Errorf("pollInterval = %v, want 1s", s.PollInterval)
	}
	if s.BatchSize != 100 {
		t.Errorf("batchSize = %d, want 100", s.BatchSize)
	}
	if s.LeaseDuration != 30*time.Second {
		t.Errorf("leaseDuration = %v, want 30s", s.LeaseDuration)
	}
	if s.Concurrency != 1 {
		t.Errorf("concurrency = %d, want 1", s.Concurrency)
	}
	if s.MaxAttempts != 10 {
		t.Errorf("maxAttempts = %d, want 10", s.MaxAttempts)
	}
	if !s.Notify {
		t.Error("notify should default true")
	}
	if s.Hooks == nil {
		t.Error("hooks should default to NoopHooks")
	}
	if s.Logger == nil {
		t.Error("logger should default to a non-nil no-op logger")
	}
	if s.Fatal == nil || s.Fatal(errors.New("x")) {
		t.Error("fatal classifier should default to never fatal")
	}
}

func TestWithFatal(t *testing.T) {
	t.Parallel()
	if s := Resolve(WithFatal(nil)); s.Fatal == nil {
		t.Fatal("nil classifier must keep the default")
	}
	if s := Resolve(WithFatal(func(error) bool { return true })); !s.Fatal(errors.New("x")) {
		t.Fatal("classifier not applied")
	}
}

func TestResolveAppliesOptions(t *testing.T) {
	s := Resolve(
		WithInstanceID("pod-1"),
		WithSchema("events"),
		WithPollInterval(2*time.Second),
		WithBatchSize(50),
		WithLeaseDuration(time.Minute),
		WithConcurrency(4),
		WithMaxAttempts(7),
		WithRetention(24*time.Hour),
		WithCleanupInterval(time.Hour),
		WithOrdered(true),
		WithoutNotify(),
		WithWaitRecheck(5*time.Second),
		WithSettleLinger(2*time.Millisecond),
	)
	if s.InstanceID != "pod-1" || s.Schema != "events" || s.BatchSize != 50 ||
		s.Concurrency != 4 || s.MaxAttempts != 7 || !s.Ordered || s.Notify ||
		s.Retention != 24*time.Hour || s.CleanupInterval != time.Hour || s.WaitRecheck != 5*time.Second ||
		s.SettleLinger != 2*time.Millisecond {
		t.Fatalf("options not applied: %+v", s)
	}
}

func TestWithConfig(t *testing.T) {
	c := Config{
		Schema:        "events",
		PollInterval:  2 * time.Second,
		BatchSize:     33,
		LeaseDuration: time.Minute,
		Concurrency:   3,
		MaxAttempts:   5,
		Ordered:       true,
		DisableNotify: true,
		WaitRecheck:   3 * time.Second,
		SettleLinger:  4 * time.Millisecond,
	}
	s := Resolve(WithConfig(c))
	if s.Schema != "events" || s.BatchSize != 33 ||
		s.Concurrency != 3 || s.MaxAttempts != 5 || !s.Ordered || s.Notify || s.WaitRecheck != 3*time.Second ||
		s.SettleLinger != 4*time.Millisecond {
		t.Fatalf("WithConfig not applied: %+v", s)
	}
}

// A zero-value Config (no loader defaults applied) must still resolve to valid,
// runnable Settings — no empty schema, no panicking ticker, NOTIFY left on.
func TestWithConfigZeroValueIsSafe(t *testing.T) {
	s := Resolve(WithConfig(Config{}))
	d := Default()
	if s.Schema != d.Schema || s.PollInterval != d.PollInterval ||
		s.LeaseDuration != d.LeaseDuration || s.BatchSize != d.BatchSize ||
		s.Concurrency != d.Concurrency || s.MaxAttempts != d.MaxAttempts || !s.Notify ||
		s.WaitRecheck != d.WaitRecheck || s.SettleLinger != d.SettleLinger {
		t.Fatalf("zero-value Config did not fall back to defaults: %+v", s)
	}
}

func TestResolveInstanceID(t *testing.T) {
	// explicit id wins
	if s := Resolve(WithInstanceID("pod-1")); s.InstanceID != "pod-1" {
		t.Fatalf("instanceID = %q, want pod-1", s.InstanceID)
	}
	// absent -> auto-generated, non-empty, and unique per Resolve
	a := Resolve()
	b := Resolve()
	if a.InstanceID == "" || b.InstanceID == "" {
		t.Fatal("auto-generated instanceID is empty")
	}
	if a.InstanceID == b.InstanceID {
		t.Fatalf("auto-generated instanceID not unique: %q", a.InstanceID)
	}
}

func TestResolveBoundsClamp(t *testing.T) {
	d := Default()
	s := Resolve(WithBatchSize(0), WithConcurrency(0), WithPollInterval(0),
		WithLeaseDuration(0), WithMaxAttempts(0), WithSchema(""), WithWaitRecheck(0), WithSettleLinger(0))
	if s.BatchSize != d.BatchSize || s.Concurrency != d.Concurrency ||
		s.PollInterval != d.PollInterval || s.LeaseDuration != d.LeaseDuration ||
		s.MaxAttempts != d.MaxAttempts || s.Schema != d.Schema || s.WaitRecheck != d.WaitRecheck ||
		s.SettleLinger != d.SettleLinger {
		t.Fatalf("bounds not clamped to defaults: %+v", s)
	}
}

func TestValidSchema(t *testing.T) {
	for _, ok := range []string{"public", "events", "_x", "a1_b"} {
		if !ValidSchema(ok) {
			t.Errorf("ValidSchema(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{"", "1abc", "a-b", "a b", "a;b", `a"b`, "пуб"} {
		if ValidSchema(bad) {
			t.Errorf("ValidSchema(%q) = true, want false", bad)
		}
	}
}

func TestDeliveryControls(t *testing.T) {
	s := Resolve(WithConfig(Config{
		DeadRetention: time.Hour, DiscardRetention: time.Minute,
		ClearPayloadOnComplete: true, PublishTimeout: time.Second,
	}), WithDeadRetention(2*time.Hour))
	if s.DeadRetention != 2*time.Hour || s.DiscardRetention != time.Minute ||
		!s.ClearPayloadOnComplete || s.PublishTimeout != time.Second {
		t.Fatalf("delivery controls not applied: %+v", s)
	}
	d := Default()
	if d.DeadRetention != 0 || d.DiscardRetention != 0 || d.ClearPayloadOnComplete || d.PublishTimeout != 0 {
		t.Fatalf("existing users must opt in: %+v", d)
	}
}
