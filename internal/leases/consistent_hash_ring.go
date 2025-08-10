package leases

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
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
	var ticker = time.NewTicker(ring.cfg.HeartbeatInterval)

	if err := ring.placeAllVNodes(ctx); err != nil {
		return err
	}

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

func (ring *ConsistentHashRing) placeAllVNodes(ctx context.Context) error {
	var g, placeCtx = errgroup.WithContext(ctx)

	for range ring.cfg.VNodeCount {
		g.Go(func() error {
			return ring.placeVNode(placeCtx)
		})
	}

	return g.Wait()
}

func (ring *ConsistentHashRing) placeVNode(ctx context.Context) error {
	_ = newVNode(ring.cfg.From, ring.cfg.To)
	return nil
}
