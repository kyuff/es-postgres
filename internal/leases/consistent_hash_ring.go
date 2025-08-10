package leases

import (
	"context"
	"time"
)

type ConsistentHashRing struct {
	cfg    Config
	values []uint32
}

func NewConsistentHashRing(cfg Config) *ConsistentHashRing {
	values := make([]uint32, cfg.To-cfg.From)
	for i := range cfg.To - cfg.From {
		values[i] = cfg.From + uint32(i)
	}

	return &ConsistentHashRing{
		cfg:    cfg,
		values: values,
	}
}

func (ring *ConsistentHashRing) Values() []uint32 {
	return ring.values
}

// Start is blocking and keeps the Values up to date.
func (ring *ConsistentHashRing) Start(ctx context.Context) error {
	var ticker = time.NewTicker(ring.cfg.Heartbeat)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := ring.heartbeat(ctx); err != nil {
				return err
			}
		}
	}
}

func (ring *ConsistentHashRing) heartbeat(ctx context.Context) error {
	return nil
}
