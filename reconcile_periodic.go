package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/kyuff/es-postgres/internal/database"
	"golang.org/x/sync/errgroup"
)

func newReconcilePeriodic(cfg *Config, connector Connector, schema *database.Schema, valuer valuer) *reconcilePeriodic {
	return &reconcilePeriodic{
		cfg:       cfg,
		connector: connector,
		schema:    schema,
		valuer:    valuer,
	}
}

type reconcilePeriodic struct {
	cfg       *Config
	connector Connector
	schema    *database.Schema
	valuer    valuer
	processor processor
}

func (h *reconcilePeriodic) Reconcile(ctx context.Context, p processor) error {
	var ticker = time.NewTicker(h.cfg.reconcileInterval)
	var errorCount = 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := h.reconcile(ctx, p)
			if err != nil {
				errorCount++
				h.cfg.logger.ErrorfCtx(ctx, "[es/postgres] Failed to check the outbox: %s", err)
			} else {
				errorCount = 0
			}

			if errorCount > 10 {
				return fmt.Errorf("max errors from outbox reached")
			}
		}
	}

}

func (h *reconcilePeriodic) reconcile(rootCtx context.Context, p processor) error {
	ctx, cancel := context.WithTimeout(rootCtx, h.cfg.reconcileTimeout)
	defer cancel()

	conn, err := h.connector.AcquireRead(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire read: %w", err)
	}
	defer conn.Release()

	const graceWindow = time.Second
	streams, err := h.schema.SelectOutboxStreamIDs(rootCtx, conn, graceWindow, h.valuer.Values())
	if err != nil {
		return err
	}

	var g errgroup.Group
	for _, stream := range streams {
		g.Go(func() error {
			processCtx, processCancel := context.WithTimeout(rootCtx, h.cfg.processTimeout)
			defer processCancel()

			return p.Process(processCtx, stream)
		})
	}

	return g.Wait()
}
