package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/singleflight"
)

type processor interface {
	Process(ctx context.Context, stream database.Stream) error
}

func newProcessWriter(cfg *Config, connector Connector, schema *database.Schema, w es.Writer, rd reader) *processWriter {
	return &processWriter{
		cfg:       cfg,
		connector: connector,
		w:         w,
		schema:    schema,
		rd:        rd,

		single: singleflight.New[database.Stream](),
	}
}

type processWriter struct {
	cfg       *Config
	connector Connector
	w         es.Writer
	schema    *database.Schema
	rd        reader

	single *singleflight.Group[database.Stream]
}

func (p *processWriter) Process(ctx context.Context, stream database.Stream) error {
	return p.single.TryDo(stream, func() error {
		return p.w.Write(ctx, stream.Type, func(yield func(es.Event, error) bool) {
			db, err := p.connector.AcquireWriteStream(ctx, stream.Type, stream.ID)
			if err != nil {
				yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to acquire write connection: %w", err))
				return
			}
			defer db.Release()

			work, eventNumber, err := p.schema.SelectOutboxWatermark(ctx, db, stream)
			if err != nil {
				yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to read outbox watermark: %w", err))
				return
			}

			if work.Watermark == eventNumber {
				// Another process raised the watermark, abandon
				return
			}

			var (
				watermark  = work.Watermark
				retryCount = work.RetryCount
				delay      time.Duration
			)

			for event, err := range p.rd.Read(ctx, stream.Type, stream.ID, watermark) {
				if err != nil {
					yield(event, err)
					break
				}

				watermark = event.EventNumber

				if !yield(event, nil) {
					break
				}
			}

			if watermark < eventNumber {
				// failed to raise the watermark
				retryCount++
				delay = p.cfg.processBackoff(retryCount)
			}

			err = p.schema.UpdateOutboxWatermark(ctx, db, stream, delay, database.OutboxWatermark{
				Watermark:  watermark,
				RetryCount: retryCount,
			})
		})
	})
}
