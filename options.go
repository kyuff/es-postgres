package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/kyuff/es-postgres/internal/logger"
)

type Config struct {
	logger      Logger
	startCtx    func() context.Context
	tablePrefix string
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
		WithTablePrefix("es"),
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
