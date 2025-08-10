package leases

import (
	"fmt"
	"strings"
)

type Config struct {
	NodeName string
	From     uint32
	To       uint32
}

type Option func(cfg *Config)

func (cfg Config) Validate() error {
	if cfg.From >= cfg.To {
		return fmt.Errorf("leases: from (%d) must be less than to (%d)", cfg.From, cfg.To)
	}

	if strings.TrimSpace(cfg.NodeName) == "" {
		return fmt.Errorf("leases: node name must not be empty")
	}

	return nil
}

func WithRange(from, to uint32) Option {
	return func(cfg *Config) {
		cfg.From = from
		cfg.To = to
	}
}

func WithNodeName(nodeName string) Option {
	return func(cfg *Config) {
		cfg.NodeName = nodeName
	}
}
