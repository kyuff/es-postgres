package postgres

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/logger"
)

type Config struct {
	logger    Logger
	poolNew   func(ctx context.Context) (*pgxpool.Pool, error)
	poolClose func() error
	startCtx  func() context.Context
}

func (c *Config) validate() error {
	if c.logger == nil {
		return errors.New("missing logger")
	}
	if c.poolNew == nil {
		return errors.New("missing connection pool")
	}

	if c.startCtx == nil {
		return errors.New("missing start context")
	}

	return nil
}

type Option func(cfg *Config)

type Logger interface {
	InfofCtx(ctx context.Context, template string, args ...any)
	ErrorfCtx(ctx context.Context, template string, args ...any)
}

func defaultOptions() *Config {
	return applyOptions(&Config{},
		// add default options here
		WithNoopLogger(),
		WithStartContext(context.Background()),
	)

}

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

// WithDSN creates a Postgresql connection pool using the provided DSN.
// The connection pool is closed by the Storage.
func WithDSN(dsn string) Option {
	return func(opt *Config) {
		opt.poolNew = func(ctx context.Context) (*pgxpool.Pool, error) {
			return database.Connect(ctx, dsn)
		}
	}
}

// WithPool uses an existing pgxpool.Pool instance.
// It is up to the caller to manage the lifecycle of the pool.
func WithPool(pool *pgxpool.Pool) Option {
	return func(opt *Config) {
		opt.poolNew = func(ctx context.Context) (*pgxpool.Pool, error) {
			return pool, nil
		}
		opt.poolClose = func() error {
			return nil
		}
	}
}

// WithStartContext uses the provided context during initialization.
func WithStartContext(ctx context.Context) Option {
	return func(opt *Config) {
		opt.startCtx = func() context.Context {
			return ctx
		}
	}
}
