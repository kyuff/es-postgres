package leases

import (
	"context"
	"errors"
	"sync"

	"github.com/kyuff/es-postgres/internal/dbtx"
)

func NewHeartbeat(cfg *Config, schema Schema) *Heartbeat {
	return &Heartbeat{
		cfg:    cfg,
		schema: schema,
	}
}

type Heartbeat struct {
	cfg    *Config
	schema Schema

	mu     sync.RWMutex
	values []uint32
}

func (h *Heartbeat) Heartbeat(ctx context.Context, db dbtx.DBTX) error {
	ring, err := h.schema.SelectLeases(ctx, db)
	if err != nil {
		return err
	}

	report := ring.Analyze(h.cfg.NodeName, h.cfg.Range, h.cfg.VNodeCount)
	return errors.Join(
		h.approveLeases(ctx, db, report.Approve),
		h.placeVNodes(ctx, db, report.MissingCount, report.BlockedVNodes),
		h.updateTTL(ctx, db),
		h.updateValues(ctx, db, report.Values),
	)
}

func (h *Heartbeat) approveLeases(ctx context.Context, db dbtx.DBTX, approve []Info) error {
	if len(approve) == 0 {
		return nil
	}

	var vnodes []uint32
	for _, info := range approve {
		vnodes = append(vnodes, info.VNode)
	}

	return h.schema.ApproveLease(ctx, db, vnodes)
}

func (h *Heartbeat) placeVNodes(ctx context.Context, db dbtx.DBTX, missing uint32, blocked map[uint32]struct{}) error {
	for range missing {
		vnode, err := h.placeVNode(ctx, db, blocked)
		if err != nil {
			return err
		}
		blocked[vnode] = struct{}{}
	}

	return nil
}

func (h *Heartbeat) placeVNode(ctx context.Context, db dbtx.DBTX, blocked map[uint32]struct{}) (uint32, error) {
	return 0, nil
}

func (h *Heartbeat) updateTTL(ctx context.Context, db dbtx.DBTX) error {
	return nil
}

func (h *Heartbeat) updateValues(ctx context.Context, db dbtx.DBTX, values []uint32) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.values = values

	return nil
}

func (h *Heartbeat) Values() []uint32 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.values
}
