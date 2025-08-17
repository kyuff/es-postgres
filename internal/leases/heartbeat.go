package leases

import (
	"context"
	"errors"
	"fmt"
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
	ring, err := h.schema.RefreshLeases(ctx, db, h.cfg.NodeName, h.cfg.LeaseTTL)
	if err != nil {
		return err
	}

	report := ring.Analyze(h.cfg.NodeName, h.cfg.Range, h.cfg.VNodeCount)

	return errors.Join(
		h.approveLeases(ctx, db, report.Approve),
		h.placeVNodes(ctx, db, report.MissingCount, report.UsedVNodes),
		h.updateValues(ctx, db, report.Values),
	)
}

func (h *Heartbeat) approveLeases(ctx context.Context, db dbtx.DBTX, approve []VNode) error {
	if len(approve) == 0 {
		return nil
	}

	var vnodes []uint32
	for _, info := range approve {
		vnodes = append(vnodes, info.VNode)
	}

	return h.schema.ApproveLease(ctx, db, vnodes)
}

func (h *Heartbeat) placeVNodes(ctx context.Context, db dbtx.DBTX, missing uint32, used vnodeSet) error {
	for range missing {
		vnode, err := h.placeVNode(ctx, db, used)
		if err != nil {
			return err
		}
		used.add(vnode)
	}

	return nil
}

func (h *Heartbeat) placeVNode(ctx context.Context, db dbtx.DBTX, used vnodeSet) (uint32, error) {
	vnode, err := h.findVNode(used)
	if err != nil {
		return 0, err
	}

	err = h.schema.InsertLease(ctx, db, vnode, h.cfg.NodeName, h.cfg.LeaseTTL, Pending.String())
	if err != nil {
		return 0, err
	}

	return vnode, nil
}

func (h *Heartbeat) findVNode(used vnodeSet) (uint32, error) {
	if len(used) >= int(h.cfg.Range.Len()) {
		return 0, fmt.Errorf("no vnodes available")
	}

	for {
		vnode := h.cfg.Range.VNode(h.cfg.Rand)
		if !used.has(vnode) {
			return vnode, nil
		}
	}
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
