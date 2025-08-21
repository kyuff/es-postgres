package processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/singleflight"
)

func New(connector Connector, schema Schema, w es.Writer, rd Reader, backoff func(streamType string, retryCount int64) time.Duration) *Processor {
	return &Processor{
		connector: connector,
		w:         w,
		schema:    schema,
		rd:        rd,
		backoff:   backoff,

		single: singleflight.New[es.StreamReference](),
	}
}

type Processor struct {
	backoff   func(streamType string, retryCount int64) time.Duration
	connector Connector
	w         es.Writer
	schema    Schema
	rd        Reader

	single *singleflight.Group[es.StreamReference]
}

func (p *Processor) Process(ctx context.Context, stream es.StreamReference) (err error) {
	defer func() {
		if m := recover(); m != nil {
			err = errors.Join(err, fmt.Errorf("panic: %v", m))
		}
	}()

	var writeErr error
	var tryErr = p.single.TryDo(stream, func() error {
		return p.w.Write(ctx, stream.StreamType, func(yield func(es.Event, error) bool) {
			db, err := p.connector.AcquireWriteStream(ctx, stream.StreamType, stream.StoreStreamID)
			if err != nil {
				writeErr = fmt.Errorf("[es/postgres] Failed to acquire write connection: %w", err)
				return
			}
			defer db.Release()

			work, eventNumber, err := p.schema.SelectOutboxWatermark(ctx, db, stream)
			if err != nil {
				writeErr = fmt.Errorf("[es/postgres] Failed to read outbox watermark: %w", err)
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

			for event, err := range p.rd.Read(ctx, stream.StreamType, work.StreamID, watermark) {
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
				delay = p.backoff(stream.StreamType, retryCount)
				watermark = work.Watermark
			}

			err = p.schema.UpdateOutboxWatermark(ctx, db, stream, delay, database.OutboxWatermark{
				Watermark:  watermark,
				RetryCount: retryCount,
				StreamID:   work.StreamID,
			})
		})
	})

	return errors.Join(err, writeErr, tryErr)
}
