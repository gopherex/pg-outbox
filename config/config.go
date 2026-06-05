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
	InstanceID      string
	Schema          string
	PollInterval    time.Duration
	BatchSize       int
	LeaseDuration   time.Duration
	Concurrency     int
	MaxAttempts     int
	Backoff         backoff.Backoff
	Retention       time.Duration
	CleanupInterval time.Duration
	Ordered         bool
	Notify          bool
	ListenPool      *pgxpool.Pool
	Hooks           port.Hooks
	Codec           port.Codec
	Logger          *slog.Logger
}

// Default returns the baseline settings before any Option is applied.
func Default() Settings {
	return Settings{
		Schema:        "public",
		PollInterval:  time.Second,
		BatchSize:     100,
		LeaseDuration: 30 * time.Second,
		Concurrency:   1,
		MaxAttempts:   10,
		Backoff:       backoff.Default,
		Notify:        true,
		Hooks:         port.NoopHooks{},
		Logger:        slog.New(slog.DiscardHandler), // no-op until the caller supplies one
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
	if s.Hooks == nil {
		s.Hooks = d.Hooks
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
	Schema          string        `mapstructure:"schema" default:"public"`
	PollInterval    time.Duration `mapstructure:"poll_interval" default:"1s"`
	BatchSize       int           `mapstructure:"batch_size" default:"100"`
	LeaseDuration   time.Duration `mapstructure:"lease_duration" default:"30s"`
	Concurrency     int           `mapstructure:"concurrency" default:"1"`
	MaxAttempts     int           `mapstructure:"max_attempts" default:"10"`
	Retention       time.Duration `mapstructure:"retention"`
	CleanupInterval time.Duration `mapstructure:"cleanup_interval"`
	Ordered         bool          `mapstructure:"ordered"`
	// DisableNotify turns LISTEN/NOTIFY off (poll-only). The zero value keeps
	// NOTIFY enabled, so a hand-built Config does not silently disable it.
	DisableNotify bool `mapstructure:"disable_notify"`
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
		s.CleanupInterval = c.CleanupInterval
		s.Ordered = c.Ordered
		s.Notify = !c.DisableNotify
	}
}

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

// WithRetention enables cleanup of published rows older than ttl. The cleaner
// also needs WithCleanupInterval to run.
func WithRetention(ttl time.Duration) Option { return func(s *Settings) { s.Retention = ttl } }

// WithCleanupInterval sets how often the cleaner runs (default off).
func WithCleanupInterval(d time.Duration) Option { return func(s *Settings) { s.CleanupInterval = d } }

// WithOrdered enables strict per-partition_key ordering.
func WithOrdered(v bool) Option { return func(s *Settings) { s.Ordered = v } }

// WithoutNotify disables LISTEN/NOTIFY wake-ups (poll-only).
func WithoutNotify() Option { return func(s *Settings) { s.Notify = false } }

// WithListenPool supplies the *pgxpool.Pool used for LISTEN/NOTIFY wake-ups.
// Without it (and unless the relay executor is itself a *pgxpool.Pool), the
// relay falls back to polling only.
func WithListenPool(p *pgxpool.Pool) Option { return func(s *Settings) { s.ListenPool = p } }

// WithHooks sets the observability hooks (default NoopHooks).
func WithHooks(h port.Hooks) Option {
	return func(s *Settings) {
		if h != nil {
			s.Hooks = h
		}
	}
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
