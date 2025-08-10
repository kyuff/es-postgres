package leases

import "fmt"

type Config struct {
	From uint32
	To   uint32
}

type Option func(cfg *Config)

func (cfg Config) Validate() error {
	if cfg.From >= cfg.To {
		return fmt.Errorf("leases: from (%d) must be less than to (%d)", cfg.From, cfg.To)
	}

	return nil
}

func WithRange(from, to uint32) Option {
	return func(cfg *Config) {
		cfg.From = from
		cfg.To = to
	}
}
