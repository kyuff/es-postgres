package eventsio

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"

	"github.com/kyuff/es"
)

func NewReader(connector Connector, schema Schema, codec Codec) *Reader {
	return &Reader{
		connector: connector,
		schema:    schema,
		codec:     codec,
	}
}

type Reader struct {
	connector Connector
	schema    Schema
	codec     Codec
}

func (rd *Reader) Read(ctx context.Context, streamType, streamID string, eventNumber int64) iter.Seq2[es.Event, error] {
	return func(yield func(es.Event, error) bool) {
		db, err := rd.connector.AcquireReadStream(ctx, streamType, streamID)
		if err != nil {
			yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to acquire read connection: %w", err))
			return
		}
		defer db.Release()

		rows, err := rd.schema.SelectEvents(ctx, db, streamType, streamID, eventNumber)
		if err != nil {
			yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to select events for %s.%s [%d]: %w",
				streamType,
				streamID,
				eventNumber,
				err,
			))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var event es.Event
			var content []byte
			var contentName string
			var metadata json.RawMessage // TODO
			err := rows.Scan(
				&event.StreamType,
				&event.StreamID,
				&event.EventNumber,
				&event.EventTime,
				&event.StoreEventID,
				&event.StoreStreamID,
				&contentName,
				&content,
				&metadata,
			)
			if err != nil {
				yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to scan events for %s.%s [%d]: %w",
					streamType,
					streamID,
					eventNumber,
					err,
				))
				return
			}

			event.Content, err = rd.codec.Decode(event.StreamType, contentName, content)
			if err != nil {
				yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to decode event %q for %s.%s [%d]: %w",
					contentName,
					streamType,
					streamID,
					eventNumber,
					err,
				))
				return
			}

			if !yield(event, nil) {
				return
			}
		}
	}
}
