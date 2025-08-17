package processor_test

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/processor"
	"github.com/kyuff/es-postgres/internal/seqs"
	"github.com/kyuff/es-postgres/internal/testdata"
	"github.com/kyuff/es-postgres/internal/uuid"
)

func TestProcess(t *testing.T) {

	var (
		eventsEqual = func(t *testing.T) func(expected, got assert.KeyValue[es.Event, error]) bool {
			return func(expected, got assert.KeyValue[es.Event, error]) bool {
				return assert.Equal(t, expected.Key.StreamID, got.Key.StreamID) &&
					assert.Equal(t, expected.Key.StreamType, got.Key.StreamType) &&
					assert.Equal(t, expected.Key.StoreEventID, got.Key.StoreEventID) &&
					assert.Equal(t, expected.Key.StoreStreamID, got.Key.StoreStreamID) &&
					assert.Equal(t, expected.Key.EventNumber, got.Key.EventNumber) &&
					assert.Equal(t, expected.Key.EventTime.String(), got.Key.EventTime.String()) &&
					assert.Equal(t, expected.Key.Content, got.Key.Content)
			}
		}
		newBackoff = func(d time.Duration) func(streamType string, retries int64) time.Duration {
			return func(streamType string, retries int64) time.Duration {
				return d
			}
		}
		failAfterWriter = func(after int) func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			return func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
				count := 0
				for range events {
					if count >= after {
						return errors.New("fail")
					}
					count++
				}

				return nil
			}
		}

		failDirectWriter = func() func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			return func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
				return errors.New("fail")
			}
		}

		acquireWriteStream = func(conn *pgxpool.Conn, err error) func(ctx context.Context, streamType string, streamID string) (*pgxpool.Conn, error) {
			return func(ctx context.Context, streamType string, streamID string) (*pgxpool.Conn, error) {
				return conn, err
			}
		}

		selectWatermark = func(w database.OutboxWatermark, eventNumber int64, err error) func(ctx context.Context, db dbtx.DBTX, stream database.Stream) (database.OutboxWatermark, int64, error) {
			return func(ctx context.Context, db dbtx.DBTX, stream database.Stream) (database.OutboxWatermark, int64, error) {
				return w, eventNumber, err
			}
		}

		newWatermark = func(watermark, retryCount int64) database.OutboxWatermark {
			return database.OutboxWatermark{
				Watermark:  watermark,
				RetryCount: retryCount,
				StreamID:   uuid.V7(),
			}
		}

		updateWatermark = func(err error) func(ctx context.Context, db dbtx.DBTX, stream database.Stream, delay time.Duration, watermark database.OutboxWatermark) error {
			return func(ctx context.Context, db dbtx.DBTX, stream database.Stream, delay time.Duration, watermark database.OutboxWatermark) error {
				return err
			}
		}

		readerFunc = func(streamType, streamID string, events []es.Event, errs ...error) func(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[es.Event, error] {
			return func(ctx context.Context, typ string, id string, eventNumber int64) iter.Seq2[es.Event, error] {
				assert.Equalf(t, streamType, typ, "streamType")
				assert.Equalf(t, streamID, id, "streamType")
				return seqs.Concat2(seqs.Seq2(events...), seqs.Error2[es.Event](errs...))
			}
		}
	)

	_ = failAfterWriter
	_ = eventsEqual

	t.Run("fail immediately with the writer", func(t *testing.T) {
		// arrange
		var (
			conn    = &ConnectorMock{}
			schema  = &SchemaMock{}
			w       = &WriterMock{}
			rd      = &ReaderMock{}
			backoff = newBackoff(time.Millisecond)
			p       = processor.New(conn, schema, w, rd, backoff)

			stream = testdata.Stream()
		)

		w.WriteFunc = failDirectWriter()

		// act
		err := p.Process(t.Context(), stream)

		// assert
		assert.Error(t, err)
	})

	t.Run("fail after a few events", func(t *testing.T) {
		// arrange
		var (
			conn    = &ConnectorMock{}
			schema  = &SchemaMock{}
			w       = &WriterMock{}
			rd      = &ReaderMock{}
			backoff = newBackoff(time.Millisecond)
			p       = processor.New(conn, schema, w, rd, backoff)

			stream    = testdata.Stream()
			watermark = newWatermark(0, 0)
			events    = testdata.Events(3, func(e *es.Event) {
				e.StreamType = stream.Type
				e.StoreStreamID = stream.StoreID
			})
			got []es.Event
		)

		conn.AcquireWriteStreamFunc = acquireWriteStream(&pgxpool.Conn{}, nil)
		schema.SelectOutboxWatermarkFunc = selectWatermark(watermark, 3, nil)
		schema.UpdateOutboxWatermarkFunc = updateWatermark(nil)
		rd.ReadFunc = readerFunc(stream.Type, watermark.StreamID, events)

		w.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			for event := range events {
				got = append(got, event)
				if len(got) >= 1 {
					return errors.New("fail")
				}
			}

			return nil
		}

		// act
		err := p.Process(t.Context(), stream)

		// assert
		assert.Error(t, err)
		assert.EqualSlice(t, events[0:1], got)
		if assert.Equal(t, 1, len(schema.UpdateOutboxWatermarkCalls())) {
			assert.Equal(t,
				database.OutboxWatermark{
					Watermark:  watermark.Watermark,
					RetryCount: watermark.RetryCount + 1,
					StreamID:   watermark.StreamID,
				},
				schema.UpdateOutboxWatermarkCalls()[0].Watermark,
			)
		}
	})

	t.Run("fail after acquire", func(t *testing.T) {
		// arrange
		var (
			conn    = &ConnectorMock{}
			schema  = &SchemaMock{}
			w       = &WriterMock{}
			rd      = &ReaderMock{}
			backoff = newBackoff(time.Millisecond)
			p       = processor.New(conn, schema, w, rd, backoff)

			stream    = testdata.Stream()
			watermark = newWatermark(3, 0)
			events    = testdata.Events(10, func(e *es.Event) {
				e.StreamType = stream.Type
				e.StoreStreamID = stream.StoreID
			})
		)

		conn.AcquireWriteStreamFunc = acquireWriteStream(nil, errors.New("fail"))
		schema.SelectOutboxWatermarkFunc = selectWatermark(watermark, 0, nil)
		schema.UpdateOutboxWatermarkFunc = updateWatermark(nil)
		rd.ReadFunc = readerFunc(stream.Type, watermark.StreamID, events)

		w.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			for range events {
			}

			return nil
		}

		// act
		err := p.Process(t.Context(), stream)

		// assert
		assert.Error(t, err)
	})

	t.Run("fail after select", func(t *testing.T) {
		// arrange
		var (
			conn    = &ConnectorMock{}
			schema  = &SchemaMock{}
			w       = &WriterMock{}
			rd      = &ReaderMock{}
			backoff = newBackoff(time.Millisecond)
			p       = processor.New(conn, schema, w, rd, backoff)

			stream    = testdata.Stream()
			watermark = newWatermark(3, 0)
			events    = testdata.Events(10, func(e *es.Event) {
				e.StreamType = stream.Type
				e.StoreStreamID = stream.StoreID
			})
		)

		conn.AcquireWriteStreamFunc = acquireWriteStream(&pgxpool.Conn{}, nil)
		schema.SelectOutboxWatermarkFunc = selectWatermark(watermark, 0, errors.New("fail"))
		schema.UpdateOutboxWatermarkFunc = updateWatermark(nil)
		rd.ReadFunc = readerFunc(stream.Type, watermark.StreamID, events)

		w.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			for range events {
			}

			return nil
		}

		// act
		err := p.Process(t.Context(), stream)

		// assert
		assert.Error(t, err)
	})

	t.Run("fail after read", func(t *testing.T) {
		// arrange
		var (
			conn    = &ConnectorMock{}
			schema  = &SchemaMock{}
			w       = &WriterMock{}
			rd      = &ReaderMock{}
			backoff = newBackoff(time.Millisecond)
			p       = processor.New(conn, schema, w, rd, backoff)

			stream    = testdata.Stream()
			watermark = newWatermark(0, 0)
			events    = testdata.Events(9, func(e *es.Event) {
				e.StreamType = stream.Type
				e.StoreStreamID = stream.StoreID
			})
			got []es.Event
		)

		conn.AcquireWriteStreamFunc = acquireWriteStream(&pgxpool.Conn{}, nil)
		schema.SelectOutboxWatermarkFunc = selectWatermark(watermark, 10, nil)
		schema.UpdateOutboxWatermarkFunc = updateWatermark(nil)
		rd.ReadFunc = readerFunc(stream.Type, watermark.StreamID, events, errors.New("test"))

		w.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			for event, err := range events {
				if err != nil {
					return err
				}
				got = append(got, event)
			}

			return nil
		}

		// act
		err := p.Process(t.Context(), stream)

		// assert
		assert.Error(t, err)
		assert.EqualSlice(t, events, got)
		if assert.Equal(t, 1, len(schema.UpdateOutboxWatermarkCalls())) {
			assert.Equal(t,
				database.OutboxWatermark{
					Watermark:  watermark.Watermark,
					RetryCount: watermark.RetryCount + 1,
					StreamID:   watermark.StreamID,
				},
				schema.UpdateOutboxWatermarkCalls()[0].Watermark,
			)
		}
	})

	t.Run("fail after a write panic", func(t *testing.T) {
		// arrange
		var (
			conn    = &ConnectorMock{}
			schema  = &SchemaMock{}
			w       = &WriterMock{}
			rd      = &ReaderMock{}
			backoff = newBackoff(time.Millisecond)
			p       = processor.New(conn, schema, w, rd, backoff)

			stream    = testdata.Stream()
			watermark = newWatermark(3, 0)
		)

		conn.AcquireWriteStreamFunc = acquireWriteStream(&pgxpool.Conn{}, nil)
		schema.SelectOutboxWatermarkFunc = selectWatermark(watermark, 0, nil)
		schema.UpdateOutboxWatermarkFunc = updateWatermark(nil)
		rd.ReadFunc = readerFunc(stream.Type, watermark.StreamID, testdata.Events(10, func(e *es.Event) {
			e.StreamType = stream.Type
			e.StoreStreamID = stream.StoreID
		}))

		w.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			panic("fail")
		}

		assert.NoPanic(t, func() {
			// act
			err := p.Process(t.Context(), stream)

			// assert
			assert.Error(t, err)
		})

	})

	t.Run("bail if other process had worked", func(t *testing.T) {
		// arrange
		var (
			conn    = &ConnectorMock{}
			schema  = &SchemaMock{}
			w       = &WriterMock{}
			rd      = &ReaderMock{}
			backoff = newBackoff(time.Millisecond)
			p       = processor.New(conn, schema, w, rd, backoff)

			stream    = testdata.Stream()
			watermark = newWatermark(10, 0)
		)

		conn.AcquireWriteStreamFunc = acquireWriteStream(&pgxpool.Conn{}, nil)
		schema.SelectOutboxWatermarkFunc = selectWatermark(watermark, 10, nil)
		schema.UpdateOutboxWatermarkFunc = updateWatermark(nil)
		rd.ReadFunc = readerFunc(stream.Type, watermark.StreamID, nil)

		w.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			for range events {
			}

			return nil
		}

		// act
		err := p.Process(t.Context(), stream)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 0, len(rd.ReadCalls()))
		assert.Equal(t, 0, len(schema.UpdateOutboxWatermarkCalls()))
	})

	t.Run("read all events", func(t *testing.T) {
		// arrange
		var (
			conn    = &ConnectorMock{}
			schema  = &SchemaMock{}
			w       = &WriterMock{}
			rd      = &ReaderMock{}
			backoff = newBackoff(time.Millisecond)
			p       = processor.New(conn, schema, w, rd, backoff)

			stream    = testdata.Stream()
			watermark = newWatermark(0, 0)
			events    = testdata.Events(10, func(e *es.Event) {
				e.StreamType = stream.Type
				e.StoreStreamID = stream.StoreID
			})
			got []es.Event
		)

		conn.AcquireWriteStreamFunc = acquireWriteStream(&pgxpool.Conn{}, nil)
		schema.SelectOutboxWatermarkFunc = selectWatermark(watermark, 5, nil)
		schema.UpdateOutboxWatermarkFunc = updateWatermark(nil)
		rd.ReadFunc = readerFunc(stream.Type, watermark.StreamID, events)

		w.WriteFunc = func(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
			for event := range events {
				got = append(got, event)
			}

			return nil
		}

		// act
		err := p.Process(t.Context(), stream)

		// assert
		assert.NoError(t, err)
		assert.EqualSlice(t, events, got)
		if assert.Equal(t, 1, len(schema.UpdateOutboxWatermarkCalls())) {
			assert.Equal(t,
				database.OutboxWatermark{
					Watermark:  10,
					RetryCount: 0,
					StreamID:   watermark.StreamID,
				},
				schema.UpdateOutboxWatermarkCalls()[0].Watermark,
			)
		}
	})
}
