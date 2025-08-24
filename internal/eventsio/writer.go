package eventsio

import (
	"context"
	"fmt"
	"iter"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/listenerpayload"
)

func NewWriter(schema Schema, validator Validator, codec Codec, partitioner func(streamType, streamID string) uint32) *Writer {
	return &Writer{
		schema:    schema,
		validator: validator,
		codec:     codec,

		partitioner: partitioner,
	}
}

type Writer struct {
	schema    Schema
	validator Validator
	codec     Codec

	partitioner func(streamType, streamID string) uint32
}

func (w *Writer) Write(ctx context.Context, db dbtx.DBTX, streamType string, events iter.Seq2[es.Event, error]) error {
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

		content, err := w.codec.Encode(event)
		if err != nil {
			return fmt.Errorf("[es/postgres] Failed to encode event: %w", err)
		}

		var metadata []byte // TODO

		err = w.schema.WriteEvent(ctx, db, event, content, metadata)
		if err != nil {
			return fmt.Errorf("[es/postgres] Failed to write event: %w", err)
		}

		lastEvent = event
		eventCount++
	}

	if eventCount == 0 {
		return nil // nothing was done
	}

	var partition = w.partitioner(streamType, lastEvent.StreamID)
	var affected int64
	var err error
	if firstEvent.EventNumber == 1 {
		affected, err = w.schema.InsertOutbox(ctx, db,
			streamType,
			lastEvent.StreamID,
			lastEvent.StoreStreamID,
			lastEvent.EventNumber,
			firstEvent.EventNumber-1,
			partition,
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

	payload := listenerpayload.Encode(streamType, lastEvent.StreamID, lastEvent.StoreStreamID)
	err = w.schema.Notify(ctx, db, partition, payload)
	if err != nil {
		return err
	}

	return nil
}

func channelName(prefix string, partition uint32) string {
	return ""
}
