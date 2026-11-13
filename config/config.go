// Package config holds the outbox runtime settings, the functional options that
// build them, and the declarative Config struct (mapstructure/validate/default
// tags) for loading from files or environment.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"regexp"
	"time"

	"github.com/gopherex/pg-outbox/backoff"
	"github.com/gopherex/pg-outbox/port"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInvalidSchema is returned when the schema is not a valid identifier.
var ErrInvalidSchema = errors.New("outbox: invalid schema identifier")

// Settings is the resolved runtime configuration the engine reads. Build it with
// Default plus Options, or from a declarative Config via WithConfig.
type Settings struct {
	InstanceID             string
	Schema                 string
	PollInterval           time.Duration
	BatchSize              int
	LeaseDuration          time.Duration
	Concurrency            int
	MaxAttempts            int
	Backoff                backoff.Backoff
	Retention              time.Duration
	DeadRetention          time.Duration
	DiscardRetention       time.Duration
	ClearPayloadOnComplete bool
	PublishTimeout         time.Duration
	CleanupInterval        time.Duration
	Ordered                bool
	Notify                 bool
	WaitRecheck            time.Duration
	SettleLinger           time.Duration
	TombstoneRetention     time.Duration
	ListenPool             *pgxpool.Pool
	Fatal                  func(error) bool
	Hooks                  port.Hooks
	Instrumentation        port.Instrumentation
	ErrorRenderer          func(error) string
	Codec                  port.Codec
	Logger                 *slog.Logger
}

// Default returns the baseline settings before any Option is applied.
func Default() Settings {
	return Settings{
		Schema:             "public",
		PollInterval:       time.Second,
		BatchSize:          100,
		LeaseDuration:      30 * time.Second,
		Concurrency:        1,
		MaxAttempts:        10,
		Backoff:            backoff.Default,
		Notify:             true,
		WaitRecheck:        time.Second,
		SettleLinger:       time.Millisecond,
		TombstoneRetention: 30 * 24 * time.Hour,
		Fatal:              neverFatal,
		Hooks:              port.NoopHooks{},
		Instrumentation:    port.NoopInstrumentation{},
		Logger:             slog.New(slog.DiscardHandler), // no-op until the caller supplies one
	}
}

// Resolve applies opts to a fresh Default and normalizes bounds. Every
// non-positive numeric/duration field and an empty schema fall back to the
// Default value, so even a zero-value Config passed via WithConfig yields a
// valid, runnable Settings.
func Resolve(opts ...Option) Settings {
	d := Default()
	s := d
	for _, o := range opts {
		o(&s)
	}
	if s.Schema == "" {
		s.Schema = d.Schema
	}
	if s.PollInterval <= 0 {
		s.PollInterval = d.PollInterval
	}
	if s.LeaseDuration <= 0 {
		s.LeaseDuration = d.LeaseDuration
	}
	if s.WaitRecheck <= 0 {
		s.WaitRecheck = d.WaitRecheck
	}
	if s.SettleLinger <= 0 {
		s.SettleLinger = d.SettleLinger
	}
	if s.TombstoneRetention <= 0 {
		s.TombstoneRetention = d.TombstoneRetention
	}
	// A retention without a cleaner would silently keep every row: run it.
	if s.CleanupInterval <= 0 && (s.Retention > 0 || s.DeadRetention > 0 || s.DiscardRetention > 0) {
		s.CleanupInterval = DefaultCleanupInterval
	}
	if s.BatchSize < 1 {
		s.BatchSize = d.BatchSize
	}
	if s.Concurrency < 1 {
		s.Concurrency = d.Concurrency
	}
	if s.MaxAttempts < 1 {
		s.MaxAttempts = d.MaxAttempts
	}
	if s.Backoff == nil {
		s.Backoff = d.Backoff
	}
	if s.Fatal == nil {
		s.Fatal = d.Fatal
	}
	if s.Hooks == nil {
		s.Hooks = d.Hooks
	}
	if s.Instrumentation == nil {
		s.Instrumentation = d.Instrumentation
	}
	if s.Logger == nil {
		s.Logger = d.Logger
	}
	if s.InstanceID == "" {
		s.InstanceID = generateInstanceID()
	}
	return s
}

// generateInstanceID builds a unique-per-process lease owner id when the caller
// did not supply one with WithInstanceID: "<hostname>-<random>". It is stable
// for the lifetime of the Settings (generated once in Resolve).
func generateInstanceID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	suffix := hex.EncodeToString(b[:])
	if host, err := os.Hostname(); err == nil && host != "" {
		return host + "-" + suffix
	}
	return "outbox-" + suffix
}

// Config is the declarative outbox configuration, loaded from a file or
// environment (mapstructure) with validation and defaults. It mirrors the
// functional options; the logger is injected separately via WithLogger because
// it is a runtime dependency, not configuration data.
type Config struct {
	Schema                 string        `mapstructure:"schema" default:"public"`
	PollInterval           time.Duration `mapstructure:"poll_interval" default:"1s"`
	BatchSize              int           `mapstructure:"batch_size" default:"100"`
	LeaseDuration          time.Duration `mapstructure:"lease_duration" default:"30s"`
	Concurrency            int           `mapstructure:"concurrency" default:"1"`
	MaxAttempts            int           `mapstructure:"max_attempts" default:"10"`
	Retention              time.Duration `mapstructure:"retention"`
	DeadRetention          time.Duration `mapstructure:"dead_retention"`
	DiscardRetention       time.Duration `mapstructure:"discard_retention"`
	ClearPayloadOnComplete bool          `mapstructure:"clear_payload_on_complete"`
	PublishTimeout         time.Duration `mapstructure:"publish_timeout"`
	CleanupInterval        time.Duration `mapstructure:"cleanup_interval"`
	Ordered                bool          `mapstructure:"ordered"`
	// DisableNotify turns LISTEN/NOTIFY off (poll-only). The zero value keeps
	// NOTIFY enabled, so a hand-built Config does not silently disable it.
	DisableNotify bool `mapstructure:"disable_notify"`
	// WaitRecheck paces the shared state read behind WaitPublished.
	WaitRecheck time.Duration `mapstructure:"wait_recheck" default:"1s"`
	// SettleLinger is the group-commit window for early outcomes (Settle).
	SettleLinger time.Duration `mapstructure:"settle_linger" default:"1ms"`
	// TombstoneRetention keeps the outcome of removed dead/discarded rows.
	TombstoneRetention time.Duration `mapstructure:"tombstone_retention" default:"720h"`
}

// Option configures Settings.
type Option func(*Settings)

// WithConfig applies a declarative Config onto the settings. Apply it before
// any narrower Option you want to win.
func WithConfig(c Config) Option {
	return func(s *Settings) {
		s.Schema = c.Schema
		s.PollInterval = c.PollInterval
		s.BatchSize = c.BatchSize
		s.LeaseDuration = c.LeaseDuration
		s.Concurrency = c.Concurrency
		s.MaxAttempts = c.MaxAttempts
		s.Retention = c.Retention
		s.DeadRetention = c.DeadRetention
		s.DiscardRetention = c.DiscardRetention
		s.ClearPayloadOnComplete = c.ClearPayloadOnComplete
		s.PublishTimeout = c.PublishTimeout
		s.CleanupInterval = c.CleanupInterval
		s.Ordered = c.Ordered
		s.Notify = !c.DisableNotify
		s.WaitRecheck = c.WaitRecheck
		s.SettleLinger = c.SettleLinger
		s.TombstoneRetention = c.TombstoneRetention
	}
}

// WithTombstoneRetention sets how long the outcome of a removed dead or
// discarded row is kept (default 30 days): a compact id + status, no payload.
// Within it Published/WaitPublished stay exact; see the outbox docs for the
// horizon.
func WithTombstoneRetention(d time.Duration) Option {
	return func(s *Settings) { s.TombstoneRetention = d }
}

// WithSettleLinger sets the group-commit window for outcomes reported early
// with Settle (default 1ms): reports within it, or before Publish returns,
// commit in one statement. It bounds how much later than its report a row is
// released.
func WithSettleLinger(d time.Duration) Option { return func(s *Settings) { s.SettleLinger = d } }

// WithWaitRecheck sets how often WaitPublished re-reads row state when no
// notification settled it (default 1s): one query per interval for all
// waiting calls, covering a lost notification, a reconnecting listener, or a
// process that does not Run this Outbox or has no LISTEN pool.
func WithWaitRecheck(d time.Duration) Option { return func(s *Settings) { s.WaitRecheck = d } }

// WithInstanceID sets the lease owner id (locked_by). Required before Run. Must
// be unique and stable per process (e.g. the Kubernetes pod name).
func WithInstanceID(id string) Option { return func(s *Settings) { s.InstanceID = id } }

// WithSchema sets the Postgres schema the table lives in (default "public").
func WithSchema(schema string) Option { return func(s *Settings) { s.Schema = schema } }

// WithPollInterval sets how often the relay polls when idle (default 1s).
func WithPollInterval(d time.Duration) Option { return func(s *Settings) { s.PollInterval = d } }

// WithBatchSize sets the max rows claimed per cycle (default 100).
func WithBatchSize(n int) Option { return func(s *Settings) { s.BatchSize = n } }

// WithLeaseDuration sets how long a claimed row stays locked (default 30s).
func WithLeaseDuration(d time.Duration) Option { return func(s *Settings) { s.LeaseDuration = d } }

// WithConcurrency sets the number of relay worker goroutines (default 1).
func WithConcurrency(n int) Option { return func(s *Settings) { s.Concurrency = n } }

// WithMaxAttempts sets the default attempt ceiling before dead-lettering (default 10).
func WithMaxAttempts(n int) Option { return func(s *Settings) { s.MaxAttempts = n } }

// WithRetryBackoff overrides the retry backoff (default backoff.Default).
func WithRetryBackoff(b backoff.Backoff) Option {
	return func(s *Settings) {
		if b != nil {
			s.Backoff = b
		}
	}
}

// WithRetention enables cleanup of published rows older than ttl.
func WithRetention(ttl time.Duration) Option { return func(s *Settings) { s.Retention = ttl } }

// DefaultCleanupInterval is how often the cleaner runs when a retention is
// set and no WithCleanupInterval is given.
const DefaultCleanupInterval = time.Minute

// WithCleanupInterval sets how often the cleaner runs (default
// DefaultCleanupInterval once any retention is set; no retention, no cleaner).
func WithCleanupInterval(d time.Duration) Option { return func(s *Settings) { s.CleanupInterval = d } }

// WithOrdered enables strict per-partition_key ordering.
func WithOrdered(v bool) Option { return func(s *Settings) { s.Ordered = v } }

// WithoutNotify disables LISTEN/NOTIFY wake-ups (poll-only).
func WithoutNotify() Option { return func(s *Settings) { s.Notify = false } }

// WithListenPool supplies the *pgxpool.Pool used for LISTEN/NOTIFY wake-ups.
// Without it (and unless the relay executor is itself a *pgxpool.Pool), the
// relay falls back to polling only.
func WithListenPool(p *pgxpool.Pool) Option { return func(s *Settings) { s.ListenPool = p } }

// WithFatal sets the classifier consulted on every Publish error and every
// internal relay/cleaner error (claim, renew, completion, cleanup) while the
// context is live. When it returns true, Run stops and returns the error; the
// claimed batch is not completed, retried or dead-lettered and is re-claimed
// after its lease expires. Abort outcomes and recovered panics are always fatal.
// Default: nothing else is fatal.
func WithFatal(f func(error) bool) Option {
	return func(s *Settings) {
		if f != nil {
			s.Fatal = f
		}
	}
}

func neverFatal(error) bool { return false }

// WithHooks sets the observability hooks (default NoopHooks).
func WithHooks(h port.Hooks) Option {
	return func(s *Settings) {
		if h != nil {
			s.Hooks = h
		}
	}
}

// WithInstrumentation plugs telemetry into Enqueue and delivery (default
// NoopInstrumentation). See port.Instrumentation.
func WithInstrumentation(i port.Instrumentation) Option {
	return func(s *Settings) {
		if i != nil {
			s.Instrumentation = i
		}
	}
}

// WithErrorRenderer renders a failed publish's error before it is stored as
// the row's last_error (default err.Error()). Pass the renderer the service's
// logger and tracer use (xlog.NewErrorRenderer), so signed URLs or database
// values never land in the table.
func WithErrorRenderer(render func(error) string) Option {
	return func(s *Settings) { s.ErrorRenderer = render }
}

// WithCodec sets the codec used by EnqueueValue.
func WithCodec(c port.Codec) Option { return func(s *Settings) { s.Codec = c } }

// WithLogger injects the slog logger from outside (default a no-op logger that
// discards everything).
func WithLogger(l *slog.Logger) Option {
	return func(s *Settings) {
		if l != nil {
			s.Logger = l
		}
	}
}

var schemaRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidSchema reports whether s is a safe schema identifier.
func ValidSchema(s string) bool { return schemaRe.MatchString(s) }

// WithDeadRetention enables deletion of dead rows after this terminal-state age.
// Disabled by default. Each removed row leaves a tombstone (WithTombstoneRetention).
func WithDeadRetention(ttl time.Duration) Option { return func(s *Settings) { s.DeadRetention = ttl } }

// WithDiscardRetention enables deletion of discarded rows after this age.
// Disabled by default. Each removed row leaves a tombstone (WithTombstoneRetention).
func WithDiscardRetention(ttl time.Duration) Option {
	return func(s *Settings) { s.DiscardRetention = ttl }
}

// WithClearPayloadOnComplete atomically empties payload and headers when a row
// becomes published or discarded. Dead rows retain data for inspection/replay.
// Other columns (including LastError) are retained; callers must sanitize them.
func WithClearPayloadOnComplete(enabled bool) Option {
	return func(s *Settings) { s.ClearPayloadOnComplete = enabled }
}

// WithPublishTimeout limits each Publish call. Zero disables the timeout.
// Publishers must honor the supplied context; a timeout cannot undo a side effect.
func WithPublishTimeout(d time.Duration) Option { return func(s *Settings) { s.PublishTimeout = d } }
