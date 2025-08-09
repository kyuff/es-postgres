package eventsio_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/eventsio"
	"github.com/kyuff/es-postgres/internal/seqs"
	"github.com/kyuff/es-postgres/internal/testdata"
)

func TestEventWriter(t *testing.T) {
	var (
		newPartitioner = func(partition uint32) func(streamType, streamID string) uint32 {
			return func(streamType, streamID string) uint32 {
				return partition
			}
		}
	)

	t.Run("do nothing on no events", func(t *testing.T) {
		// arrange
		var (
			db          = &pgxpool.Pool{}
			schema      = &SchemaMock{}
			codec       = &CodecMock{}
			partitioner = newPartitioner(1)
			w           = eventsio.NewWriter(schema, eventsio.NewValidator(), codec, partitioner)
			streamType  = testdata.StreamType()
			events      = testdata.Events(0, func(e *es.Event) {
				e.StreamType = streamType
			})
		)

		// act
		err := w.Write(t.Context(), db, streamType, seqs.Seq2(events...))

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 0, len(schema.WriteEventCalls()))
	})

	t.Run("do nothing on validation error", func(t *testing.T) {
		// arrange
		var (
			db          = &pgxpool.Pool{}
			schema      = &SchemaMock{}
			validator   = &ValidatorMock{}
			codec       = &CodecMock{}
			partitioner = newPartitioner(1)
			w           = eventsio.NewWriter(schema, validator, codec, partitioner)
			streamType  = testdata.StreamType()
			events      = testdata.Events(0, func(e *es.Event) {
				e.StreamType = streamType
			})
		)

		validator.ValidateFunc = func(streamType string, events iter.Seq2[es.Event, error]) iter.Seq2[es.Event, error] {
			return seqs.Error2[es.Event](errors.New("fail"))
		}

		// act
		err := w.Write(t.Context(), db, streamType, seqs.Seq2(events...))

		// assert
		assert.Error(t, err)
		assert.Equal(t, 0, len(schema.WriteEventCalls()))
	})

	t.Run("fail on write error", func(t *testing.T) {

		// arrange
		var (
			db          = &pgxpool.Pool{}
			schema      = &SchemaMock{}
			validator   = eventsio.NewValidator()
			codec       = &CodecMock{}
			partitioner = newPartitioner(1)
			w           = eventsio.NewWriter(schema, validator, codec, partitioner)
			streamType  = testdata.StreamType()
			events      = testdata.Events(10, func(e *es.Event) {
				e.StreamType = streamType
			})
		)

		schema.WriteEventFunc = func(ctx context.Context, db database.DBTX, event es.Event, content, metadata []byte) error {
			return errors.New("fail")
		}

		codec.EncodeFunc = func(event es.Event) ([]byte, error) {
			return []byte(`content`), nil
		}

		// act
		err := w.Write(t.Context(), db, streamType, seqs.Seq2(events...))

		// assert
		assert.Error(t, err)
		assert.Equal(t, 1, len(schema.WriteEventCalls()))
		assert.Equal(t, 0, len(schema.InsertOutboxCalls()))
		assert.Equal(t, 0, len(schema.UpdateOutboxCalls()))
	})

	t.Run("insert first event on outbox", func(t *testing.T) {
		// arrange
		var (
			db          = &pgxpool.Pool{}
			schema      = &SchemaMock{}
			validator   = eventsio.NewValidator()
			codec       = &CodecMock{}
			partitioner = newPartitioner(1)
			w           = eventsio.NewWriter(schema, validator, codec, partitioner)
			streamType  = testdata.StreamType()
			events      = testdata.Events(10, func(e *es.Event) {
				e.StreamType = streamType
			})
		)

		schema.WriteEventFunc = func(ctx context.Context, db database.DBTX, event es.Event, content, metadata []byte) error {
			return nil
		}

		codec.EncodeFunc = func(event es.Event) ([]byte, error) {
			return []byte(`content`), nil
		}

		schema.InsertOutboxFunc = func(ctx context.Context, tx database.DBTX, typ string, id string, storeID string, eventNumber int64, watermark int64, partition uint32) (int64, error) {
			assert.Equal(t, events[eventNumber-1].StreamType, typ)
			assert.Equal(t, events[eventNumber-1].StreamID, id)
			assert.Equal(t, events[eventNumber-1].StoreStreamID, storeID)
			assert.Equal(t, events[eventNumber-1].EventNumber, eventNumber)
			return 1, nil
		}

		// act
		err := w.Write(t.Context(), db, streamType, seqs.Seq2(events...))

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 10, len(schema.WriteEventCalls()))
		assert.Equal(t, 1, len(schema.InsertOutboxCalls()))
		assert.Equal(t, 0, len(schema.UpdateOutboxCalls()))
	})

	t.Run("update followup events on outbox", func(t *testing.T) {
		// arrange
		var (
			db          = &pgxpool.Pool{}
			schema      = &SchemaMock{}
			validator   = eventsio.NewValidator()
			codec       = &CodecMock{}
			partitioner = newPartitioner(1)
			w           = eventsio.NewWriter(schema, validator, codec, partitioner)
			streamType  = testdata.StreamType()
			events      = testdata.Events(10, func(e *es.Event) {
				e.StreamType = streamType
			})
			offset int64 = 3
		)

		schema.WriteEventFunc = func(ctx context.Context, db database.DBTX, event es.Event, content, metadata []byte) error {
			return nil
		}

		schema.UpdateOutboxFunc = func(ctx context.Context, tx database.DBTX, typ string, id string, eventNumber int64, lastEventNumber int64) (int64, error) {
			assert.Equal(t, events[eventNumber-1].StreamType, typ)
			assert.Equal(t, events[eventNumber-1].StreamID, id)
			assert.Equal(t, events[eventNumber-1].EventNumber, eventNumber)
			assert.Equal(t, events[offset-1].EventNumber, lastEventNumber)
			return 1, nil
		}

		codec.EncodeFunc = func(event es.Event) ([]byte, error) {
			return []byte(`content`), nil
		}

		// act
		err := w.Write(t.Context(), db, streamType, seqs.Seq2(events[offset:]...))

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 7, len(schema.WriteEventCalls()))
		assert.Equal(t, 0, len(schema.InsertOutboxCalls()))
		assert.Equal(t, 1, len(schema.UpdateOutboxCalls()))
	})

	t.Run("fail if update effects none", func(t *testing.T) {
		// arrange
		var (
			db          = &pgxpool.Pool{}
			schema      = &SchemaMock{}
			validator   = eventsio.NewValidator()
			codec       = &CodecMock{}
			partitioner = newPartitioner(1)
			w           = eventsio.NewWriter(schema, validator, codec, partitioner)
			streamType  = testdata.StreamType()
			events      = testdata.Events(10, func(e *es.Event) {
				e.StreamType = streamType
			})
			offset int64 = 3
		)

		schema.WriteEventFunc = func(ctx context.Context, db database.DBTX, event es.Event, content, metadata []byte) error {
			return nil
		}

		schema.UpdateOutboxFunc = func(ctx context.Context, tx database.DBTX, typ string, id string, eventNumber int64, lastEventNumber int64) (int64, error) {
			return 0, nil
		}

		codec.EncodeFunc = func(event es.Event) ([]byte, error) {
			return []byte(`content`), nil
		}

		// act
		err := w.Write(t.Context(), db, streamType, seqs.Seq2(events[offset:]...))

		// assert
		assert.Error(t, err)
		assert.Equal(t, 7, len(schema.WriteEventCalls()))
		assert.Equal(t, 0, len(schema.InsertOutboxCalls()))
		assert.Equal(t, 1, len(schema.UpdateOutboxCalls()))
	})

	t.Run("fail if update effects more than one", func(t *testing.T) {
		// arrange
		var (
			db          = &pgxpool.Pool{}
			schema      = &SchemaMock{}
			validator   = eventsio.NewValidator()
			codec       = &CodecMock{}
			partitioner = newPartitioner(1)
			w           = eventsio.NewWriter(schema, validator, codec, partitioner)
			streamType  = testdata.StreamType()
			events      = testdata.Events(10, func(e *es.Event) {
				e.StreamType = streamType
			})
			offset int64 = 3
		)

		schema.WriteEventFunc = func(ctx context.Context, db database.DBTX, event es.Event, content, metadata []byte) error {
			return nil
		}

		schema.UpdateOutboxFunc = func(ctx context.Context, tx database.DBTX, typ string, id string, eventNumber int64, lastEventNumber int64) (int64, error) {
			return 2, nil
		}

		codec.EncodeFunc = func(event es.Event) ([]byte, error) {
			return []byte(`content`), nil
		}

		// act
		err := w.Write(t.Context(), db, streamType, seqs.Seq2(events[offset:]...))

		// assert
		assert.Error(t, err)
		assert.Equal(t, 7, len(schema.WriteEventCalls()))
		assert.Equal(t, 0, len(schema.InsertOutboxCalls()))
		assert.Equal(t, 1, len(schema.UpdateOutboxCalls()))
	})

	t.Run("fail if insert returns error", func(t *testing.T) {
		// arrange
		var (
			db          = &pgxpool.Pool{}
			schema      = &SchemaMock{}
			validator   = eventsio.NewValidator()
			codec       = &CodecMock{}
			partitioner = newPartitioner(1)
			w           = eventsio.NewWriter(schema, validator, codec, partitioner)
			streamType  = testdata.StreamType()
			events      = testdata.Events(10, func(e *es.Event) {
				e.StreamType = streamType
			})
		)

		schema.WriteEventFunc = func(ctx context.Context, db database.DBTX, event es.Event, content, metadata []byte) error {
			return nil
		}

		schema.InsertOutboxFunc = func(ctx context.Context, tx database.DBTX, streamType string, streamID string, storeStreamID string, eventNumber int64, watermark int64, partition uint32) (int64, error) {
			return 1, errors.New("some error")
		}

		codec.EncodeFunc = func(event es.Event) ([]byte, error) {
			return []byte(`content`), nil
		}

		// act
		err := w.Write(t.Context(), db, streamType, seqs.Seq2(events...))

		// assert
		assert.Error(t, err)
		assert.Equal(t, 10, len(schema.WriteEventCalls()))
		assert.Equal(t, 1, len(schema.InsertOutboxCalls()))
		assert.Equal(t, 0, len(schema.UpdateOutboxCalls()))
	})
}
