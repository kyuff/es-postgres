package reconcilers

import (
	"context"
	"fmt"
	"time"

	"github.com/kyuff/es-postgres/internal/retry"
	"github.com/kyuff/es-postgres/internal/uuid"
	"golang.org/x/sync/errgroup"
)

const (
	ReconcileLimit = 100
	graceWindow    = time.Millisecond * 200
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
	err := retry.Continue(ctx, h.interval, 10, func(ctx context.Context) error {
		return h.reconcile(ctx, p)
	})
	if err != nil {
		h.logger.ErrorfCtx(ctx, "[es/postgres] Failed to reconcile the outbox: %s", err)
		return err
	}

	return nil
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
			token = stream.StoreStreamID
			g.Go(func() error {
				processCtx, processCancel := context.WithTimeout(rootCtx, h.processTimeout)
				defer processCancel()

				err := p.Process(processCtx, stream)
				if err != nil {
					return err
				}

				return nil
			})
		}

		if len(streams) < ReconcileLimit {
			break
		}

	}

	return g.Wait()
}
