package reconcilers

import (
	"context"
	"fmt"
	"time"

	"github.com/kyuff/es-postgres/internal/uuid"
	"golang.org/x/sync/errgroup"
)

const (
	ReconcileLimit = 100
	graceWindow    = time.Second
)

func NewPeriodic(logger Logger, connector Connector, schema Schema, valuer Valuer, interval time.Duration, timeout time.Duration, processTimeout time.Duration) *Periodic {
	return &Periodic{
		interval:       interval,
		timeout:        timeout,
		processTimeout: processTimeout,

		logger:    logger,
		connector: connector,
		schema:    schema,
		valuer:    valuer,
	}
}

type Periodic struct {
	interval       time.Duration
	timeout        time.Duration
	processTimeout time.Duration

	logger    Logger
	connector Connector
	schema    Schema
	valuer    Valuer
}

func (h *Periodic) Reconcile(ctx context.Context, p Processor) error {
	var ticker = time.NewTicker(h.interval)
	var errorCount = 0
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return nil
		case <-ticker.C:
			err := h.reconcile(ctx, p)
			if err != nil {
				errorCount++
				h.logger.ErrorfCtx(ctx, "[es/postgres] Failed to check the outbox: %s", err)
			} else {
				errorCount = 0
			}

			if errorCount > 10 {
				return fmt.Errorf("max errors from outbox reached")
			}
		}
	}

}

func (h *Periodic) reconcile(rootCtx context.Context, p Processor) error {
	ctx, cancel := context.WithTimeout(rootCtx, h.timeout)
	defer cancel()

	conn, err := h.connector.AcquireRead(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire read: %w", err)
	}
	defer conn.Release()

	var g errgroup.Group
	var token = uuid.Empty
	for {
		streams, err := h.schema.SelectOutboxStreamIDs(rootCtx, conn, graceWindow, h.valuer.Values(), token, ReconcileLimit)
		if err != nil {
			return err
		}

		for _, stream := range streams {
			token = stream.StoreID
			g.Go(func() error {
				processCtx, processCancel := context.WithTimeout(rootCtx, h.processTimeout)
				defer processCancel()

				return p.Process(processCtx, stream)
			})
		}

		if len(streams) < ReconcileLimit {
			break
		}

	}

	return g.Wait()
}
