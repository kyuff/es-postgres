package leases

import (
	"context"
	"errors"

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
}

func (h *Heartbeat) Heartbeat(ctx context.Context, db dbtx.DBTX) error {
	ring, err := h.schema.SelectLeasesRing(ctx, db)
	if err != nil {
		return err
	}

	report := ring.Analyze(h.cfg.NodeName, h.cfg.Range, h.cfg.VNodeCount)
	return errors.Join(
		h.approveVnodes(ctx, db, report.Approve),
		h.placeVNodes(ctx, db, report.MissingCount, report.BlockedVNodes),
		h.updateTTL(ctx, db, report.Ranges),
		h.updateValues(ctx, db, report.Ranges),
	)
}

func (h *Heartbeat) approveVnodes(ctx context.Context, db dbtx.DBTX, approve []Info) error {
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

func (h *Heartbeat) updateTTL(ctx context.Context, db dbtx.DBTX, ranges []Range) error {
	return nil
}

func (h *Heartbeat) updateValues(ctx context.Context, db dbtx.DBTX, ranges []Range) error {
	return nil
}
