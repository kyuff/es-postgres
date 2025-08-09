package eventsio

import (
	"context"
	"fmt"
	"iter"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/database"
)

func NewWriter(schema Schema, validator Validator, partitioner func(streamType, streamID string) uint32) *Writer {
	return &Writer{
		schema:    schema,
		validator: validator,

		partitioner: partitioner,
	}
}

type Writer struct {
	schema    Schema
	validator Validator

	partitioner func(streamType, streamID string) uint32
}

func (w *Writer) Write(ctx context.Context, db database.DBTX, streamType string, events iter.Seq2[es.Event, error]) error {
	var (
		firstEvent es.Event
		lastEvent  es.Event
		eventCount = 0
	)

	for event, err := range w.validator.Validate(streamType, events) {
		if err != nil {
			return fmt.Errorf("[es/postgres] Range over events to be written failed: %w", err)
		}

		if eventCount == 0 {
			firstEvent = event
		}

		err = w.schema.WriteEvent(ctx, db, event)
		if err != nil {
			return fmt.Errorf("[es/postgres] Failed to write event: %w", err)
		}

		lastEvent = event
		eventCount++
	}

	if eventCount == 0 {
		return nil // nothing was done
	}

	var affected int64
	var err error
	if firstEvent.EventNumber == 1 {
		affected, err = w.schema.InsertOutbox(ctx, db,
			streamType,
			lastEvent.StreamID,
			lastEvent.StoreStreamID,
			lastEvent.EventNumber,
			firstEvent.EventNumber-1,
			w.partitioner(streamType, lastEvent.StreamID),
		)
	} else {
		affected, err = w.schema.UpdateOutbox(ctx, db,
			streamType,
			lastEvent.StreamID,
			lastEvent.EventNumber,
			firstEvent.EventNumber-1,
		)
	}
	if err != nil {
		return err
	}

	if affected != 1 {
		return fmt.Errorf("[es/postgres] Failed to update outbox for %s.%s", streamType, lastEvent.StreamID)
	}

	return nil
}
