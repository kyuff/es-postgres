package leases

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	NodeName          string
	From              uint32
	To                uint32
	VNodeCount        uint32
	HeartbeatInterval time.Duration
}

func (cfg Config) rangeLen() uint32 {
	return cfg.To - cfg.From
}

type Option func(cfg *Config)

func (cfg Config) Validate() error {
	if cfg.From >= cfg.To {
		return fmt.Errorf("leases: from (%d) must be less than to (%d)", cfg.From, cfg.To)
	}

	if strings.TrimSpace(cfg.NodeName) == "" {
		return fmt.Errorf("leases: node name must not be empty")
	}

	if cfg.VNodeCount == 0 {
		return fmt.Errorf("leases: vnode count must be greater than 0")
	}

	if cfg.VNodeCount >= cfg.rangeLen() {
		return fmt.Errorf("leases: vnode count (%d) must be less than range size (%d - %d)", cfg.VNodeCount, cfg.From, cfg.To)
	}

	if cfg.HeartbeatInterval == 0 {
		return fmt.Errorf("leases: heartbeat interval must be greater than 0")
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
