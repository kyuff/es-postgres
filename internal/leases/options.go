package leases

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	NodeName          string
	Range             Range
	VNodeCount        uint32
	HeartbeatInterval time.Duration
}

type Option func(cfg *Config)

func (cfg Config) Validate() error {
	if !cfg.Range.Valid() {
		return fmt.Errorf("leases: invalid range: %s", cfg.Range)
	}

	if strings.TrimSpace(cfg.NodeName) == "" {
		return fmt.Errorf("leases: node name must not be empty")
	}

	if cfg.VNodeCount == 0 {
		return fmt.Errorf("leases: vnode count must be greater than 0")
	}

	if cfg.VNodeCount >= cfg.Range.Len() {
		return fmt.Errorf("leases: vnode count (%d) must be less than range size (%s)", cfg.VNodeCount, cfg.Range)
	}

	if cfg.HeartbeatInterval == 0 {
		return fmt.Errorf("leases: heartbeat interval must be greater than 0")
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

func WithHeartbeatInterval(interval time.Duration) Option {
	return func(cfg *Config) {
		cfg.HeartbeatInterval = interval
	}
}
