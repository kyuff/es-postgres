package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/kyuff/es-postgres/backoff"
	"github.com/kyuff/es-postgres/internal/hash"
	"github.com/kyuff/es-postgres/internal/leases"
	"github.com/kyuff/es-postgres/internal/logger"
	"github.com/kyuff/es/codecs"
)

const (
	defaultPartitionCount = 1024
)

type Config struct {
	logger              Logger
	startCtx            func() context.Context
	tablePrefix         string
	codec               codec
	partitioner         func(streamType, streamID string) uint32
	reconcilePublishing bool
	reconcileInterval   time.Duration
	reconcileTimeout    time.Duration
	processTimeout      time.Duration
	processBackoff      func(streamType string, retryCount int64) time.Duration
	leases              leases.Config
}

func defaultOptions() *Config {
	return applyOptions(&Config{},
		// add default options here
		WithNoopLogger(),
		WithStartContext(context.Background()),
		WithTablePrefix("es"),
		withJsonCodec(),
		WithReconcileInterval(time.Second*10),
		WithReconcileTimeout(time.Second*5),
		WithProcessTimeout(time.Second*3),
		WithLinearProcessBackoff(time.Second),
		WithReconcilePublishing(true),
		WithFNVPartitioner(defaultPartitionCount),
		withLeaseRange(0, defaultPartitionCount),
		withLeaseNodeName(hash.RandomString(12)),
	)

}

var tablePrefixRE = regexp.MustCompile(`^[a-z][a-z0-9]{1,20}$`)

func (c *Config) validate() error {
	if c.logger == nil {
		return errors.New("missing logger")
	}

	if c.startCtx == nil {
		return errors.New("missing start context")
	}

	if !tablePrefixRE.MatchString(c.tablePrefix) {
		return fmt.Errorf("invalid table prefix %q, must match: %s", c.tablePrefix, tablePrefixRE.String())
	}

	if c.codec == nil {
		return errors.New("missing codec")
	}

	if c.partitioner == nil {
		return errors.New("missing partitioner")
	}

	if c.reconcileInterval <= 0 {
		return errors.New("missing check interval")
	}

	if c.processBackoff == nil {
		return errors.New("missing process backoff")
	}

	if err := c.leases.Validate(); err != nil {
		return err
	}

	return nil
}

type Option func(cfg *Config)

func applyOptions(options *Config, opts ...Option) *Config {
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func WithLogger(logger Logger) Option {
	return func(opt *Config) {
		opt.logger = logger
	}
}
func WithNoopLogger() Option {
	return WithLogger(logger.Noop{})
}

func WithDefaultSlog() Option {
	return WithSlog(slog.Default())
}

func WithSlog(log *slog.Logger) Option {
	return WithLogger(
		logger.NewSlog(log),
	)
}

// WithStartContext uses the provided context during initialization.
func WithStartContext(ctx context.Context) Option {
	return func(opt *Config) {
		opt.startCtx = func() context.Context {
			return ctx
		}
	}
}

// WithTablePrefix uses the given prefix for the database tables
// that this library creates.
// Table names will have the form "{prefix}_{name}".
// Example: "es_migrations"
func WithTablePrefix(prefix string) Option {
	return func(cfg *Config) {
		cfg.tablePrefix = prefix
	}
}

// withJsonCodec determines how data is stored in the database.
// It is kept private for now, as the underlying postgres column type is jsonb.
func withJsonCodec() Option {
	return func(cfg *Config) {
		cfg.codec = codecs.NewJSON()
	}
}

func WithPartitioner(partitioner func(streamType, streamID string) uint32) Option {
	return func(cfg *Config) {
		cfg.partitioner = partitioner
	}
}

func WithFNVPartitioner(partitionCount uint32) Option {
	return WithPartitioner(func(streamType, streamID string) uint32 {
		return hash.FNV(streamType+streamID, partitionCount)
	})
}

// WithReconcileInterval sets how often the Storage will check for new items in the Outbox.
// This is purely a fallback mechanism, as there is a life-streaming mechanism that
// should take all traffic.
// In some error scenarios this check will ensure everything is published.
// The default value is reasonable high, so as to not put too much load on the database.
func WithReconcileInterval(interval time.Duration) Option {
	return func(cfg *Config) {
		cfg.reconcileInterval = interval
	}
}

// WithReconcileTimeout sets the timeout for doing a reconcile check.
func WithReconcileTimeout(timeout time.Duration) Option {
	return func(cfg *Config) {
		cfg.reconcileTimeout = timeout
	}
}

// WithProcessTimeout sets the timeout for processing a stream in the outbox
func WithProcessTimeout(timeout time.Duration) Option {
	return func(cfg *Config) {
		cfg.processTimeout = timeout
	}
}

// WithProcessBackoff is used to delay processing of a stream in the outbox after failure
func WithProcessBackoff(fn func(streamType string, retryCount int64) time.Duration) Option {
	return func(cfg *Config) {
		cfg.processBackoff = fn
	}
}

// WithFixedProcessBackoff waits a fixed amount of time between retries.
func WithFixedProcessBackoff(d time.Duration) Option {
	return WithProcessBackoff(func(_ string, _ int64) time.Duration {
		return backoff.Fixed(d)
	})
}

// WithLinearProcessBackoff increases the wait time linearly with each retry.
func WithLinearProcessBackoff(increment time.Duration) Option {
	return WithProcessBackoff(func(_ string, retries int64) time.Duration {
		return backoff.Linear(increment, retries)
	})
}

// WithExponentialProcessBackoff doubles the wait time with each retry.
func WithExponentialProcessBackoff(base time.Duration) Option {
	return WithProcessBackoff(func(_ string, retries int64) time.Duration {
		return backoff.Exponential(base, retries)
	})
}

// WithReconcilePublishing enables publishing by a periodic reconciler.
func WithReconcilePublishing(enabled bool) Option {
	return func(cfg *Config) {
		cfg.reconcilePublishing = enabled
	}
}

// withLeaseRange sets the range of leases used by the consistent hashing mechanism.
func withLeaseRange(from, to uint32) Option {
	return func(cfg *Config) {
		leases.WithRange(from, to)(&cfg.leases)
	}
}

func withLeaseNodeName(nodeName string) Option {
	return func(cfg *Config) {
		leases.WithNodeName(nodeName)(&cfg.leases)
	}
}
