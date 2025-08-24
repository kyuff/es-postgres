package leases

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/kyuff/es-postgres/internal/hash"
)

type Config struct {
	NodeName          string
	Range             Range
	VNodeCount        uint32
	HeartbeatTimeout  time.Duration
	HeartbeatInterval time.Duration
	LeaseTTL          time.Duration
	Rand              *rand.Rand
	listener          ValueListener
}

func DefaultOptions() *Config {
	return applyOptions(&Config{},
		WithNodeName(hash.RandomString(12)),
		WithVNodeCount(5),
		WithHeartbeatTimeout(2*time.Second),
		WithHeartbeatInterval(2*time.Second),
		WithLeaseTTL(3*time.Second),
		WithRand(rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))),
		WithValueListener(noopValueListener()),
	) // add default options here

}

type Option func(cfg *Config)

func applyOptions(options *Config, opts ...Option) *Config {
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func (cfg Config) validate() error {
	if !cfg.Range.Valid() {
		return fmt.Errorf("leases: invalid range: %s", cfg.Range)
	}

	if strings.TrimSpace(cfg.NodeName) == "" {
		return fmt.Errorf("leases: node name must not be empty")
	}

	if cfg.VNodeCount == 0 {
		return fmt.Errorf("leases: vnode count must be greater than 0")
	}

	if cfg.VNodeCount > cfg.Range.Len() {
		return fmt.Errorf("leases: vnode count (%d) must be less than range size (%s)", cfg.VNodeCount, cfg.Range)
	}

	if cfg.HeartbeatInterval == 0 {
		return fmt.Errorf("leases: heartbeat interval must be greater than 0")
	}

	if cfg.Rand == nil {
		return fmt.Errorf("leases: rand must not be nil")
	}

	if cfg.listener == nil {
		return fmt.Errorf("leases: value listener must not be nil")
	}

	return nil
}

func WithRange(r Range) Option {
	return func(cfg *Config) {
		cfg.Range = r
	}
}

func WithNodeName(nodeName string) Option {
	return func(cfg *Config) {
		cfg.NodeName = nodeName
	}
}

func WithVNodeCount(count uint32) Option {
	return func(cfg *Config) {
		cfg.VNodeCount = count
	}
}

func WithHeartbeatTimeout(timeout time.Duration) Option {
	return func(cfg *Config) {
		cfg.HeartbeatTimeout = timeout
	}
}

func WithHeartbeatInterval(interval time.Duration) Option {
	return func(cfg *Config) {
		cfg.HeartbeatInterval = interval
	}
}

func WithRand(r *rand.Rand) Option {
	return func(cfg *Config) {
		cfg.Rand = r
	}
}

func WithLeaseTTL(ttl time.Duration) Option {
	return func(cfg *Config) {
		cfg.LeaseTTL = ttl
	}
}

func WithValueListener(listener ValueListener) Option {
	return func(cfg *Config) {
		cfg.listener = listener
	}
}

func noopValueListener() ValueListenerFunc {
	return func(values, added, removed []uint32) error {
		return nil
	}
}
