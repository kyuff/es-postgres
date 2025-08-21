package database_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/testdata"
)

func TestSchema(t *testing.T) {
	var (
		newPartitions = func(partitions ...uint32) []uint32 {
			return partitions
		}
		newSchema = func(t *testing.T) (*pgxpool.Pool, *database.Schema) {
			dsn := database.DSNTest(t)
			pool, err := database.Connect(t.Context(), dsn)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			schema, err := database.NewSchema("es")
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			err = database.Migrate(t.Context(), pool, schema)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			return pool, schema
		}
		writeEvents = func(t *testing.T, db dbtx.DBTX, schema *database.Schema, events []es.Event) {
			for _, event := range events {
				b, err := json.Marshal(event.Content)
				if !assert.NoError(t, err) {
					t.Fatal(err)
				}
				assert.NoError(t, schema.WriteEvent(t.Context(), db, event, b, []byte("{}")))
			}
		}
		assertEqualRefsInRows = func(t *testing.T, expected []es.StreamReference, rows pgx.Rows) {
			t.Helper()
			n := 0
			for rows.Next() {
				var got es.StreamReference
				err := rows.Scan(&got.StreamType, &got.StreamID, &got.StoreStreamID)
				assert.NoError(t, err)
				assert.Equal(t, expected[n], got)
				n++
			}
		}
		assertEqualEventsInRow = func(t *testing.T, expected []es.Event, rows pgx.Rows) {
			t.Helper()
			n := 0
			for rows.Next() {
				var got es.Event
				var contentName string
				var content json.RawMessage
				var metadata json.RawMessage
				err := rows.Scan(
					&got.StreamType,
					&got.StreamID,
					&got.EventNumber,
					&got.EventTime,
					&got.StoreEventID,
					&got.StoreStreamID,
					&contentName,
					&content,
					&metadata,
				)
				if assert.NoError(t, err) {
					assert.Equalf(t, expected[n].StreamType, got.StreamType, "StreamType")
					assert.Equalf(t, expected[n].StreamID, got.StreamID, "StreamID")
					assert.Equalf(t, expected[n].EventNumber, got.EventNumber, "EventNumber")
					assert.Equalf(t, expected[n].EventTime, got.EventTime, "EventTime")
					assert.Equalf(t, expected[n].StoreEventID, got.StoreEventID, "StoreEventID")
					assert.Equalf(t, expected[n].StoreStreamID, got.StoreStreamID, "StoreStreamID")
					assert.Equalf(t, expected[n].Content.EventName(), contentName, "Content")
					assert.NotNil(t, content)
					assert.NotNil(t, metadata)
				}
				n++
			}
			assert.Equalf(t, len(expected), n, "number of events")
		}
		insertOutbox = func(t *testing.T, conn dbtx.DBTX, schema *database.Schema, streams []es.StreamReference, eventNumber, watermark int64, partition uint32) {
			for _, stream := range streams {
				if _, err := schema.InsertOutbox(t.Context(), conn, stream.StreamType, stream.StreamID, stream.StoreStreamID, eventNumber, watermark, partition); err != nil {
					t.Fatal(err)
				}
			}
		}
	)

	t.Run("SelectCurrentMigration", func(t *testing.T) {
		t.Run("read", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
			)

			// act
			got, err := schema.SelectCurrentMigration(t.Context(), conn)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, 1, got)
		})
	})

	t.Run("WriteEvent", func(t *testing.T) {
		t.Run("succeed", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				streamID     = testdata.StreamID()
				events       = testdata.Events(1, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
				})
				content  = []byte(`{}`)
				metadata = []byte(`{}`)
			)

			// act
			err := schema.WriteEvent(t.Context(), conn, events[0], content, metadata)

			// assert
			assert.NoError(t, err)
		})

		t.Run("fail due to duplicate", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				streamID     = testdata.StreamID()
				events       = testdata.Events(1, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
				})
				content  = []byte(`{}`)
				metadata = []byte(`{}`)
			)

			assert.NoError(t, schema.WriteEvent(t.Context(), conn, events[0], content, metadata))

			// act
			err := schema.WriteEvent(t.Context(), conn, events[0], content, metadata)

			// assert
			assert.Error(t, err)
		})
	})

	t.Run("SelectEvents", func(t *testing.T) {
		t.Run("full stream", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				streamID     = testdata.StreamID()
				events       = testdata.Events(10, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
				})
			)

			writeEvents(t, conn, schema, events)

			// act
			rows, err := schema.SelectEvents(t.Context(), conn, streamType, streamID, 0)

			// assert
			assert.NoError(t, err)
			assertEqualEventsInRow(t, events, rows)
		})

		t.Run("tail of stream", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				streamID     = testdata.StreamID()
				events       = testdata.Events(10, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
				})
			)

			writeEvents(t, conn, schema, events)

			// act
			rows, err := schema.SelectEvents(t.Context(), conn, streamType, streamID, 5)

			// assert
			assert.NoError(t, err)
			assertEqualEventsInRow(t, events[5:], rows)
		})

		t.Run("empty stream", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				streamID     = testdata.StreamID()
			)

			// act
			rows, err := schema.SelectEvents(t.Context(), conn, streamType, streamID, 0)

			// assert
			assert.NoError(t, err)
			assertEqualEventsInRow(t, nil, rows)
		})
	})

	t.Run("InsertOutbox", func(t *testing.T) {
		t.Run("first insert", func(t *testing.T) {
			// arrange
			var (
				conn, schema         = newSchema(t)
				streamType           = testdata.StreamType()
				streamID             = testdata.StreamID()
				storeStreamID        = testdata.StoreStreamID()
				eventNumber   int64  = 3
				watermark     int64  = 3
				partition     uint32 = 5
			)

			// act
			affected, err := schema.InsertOutbox(t.Context(), conn, streamType, streamID, storeStreamID, eventNumber, watermark, partition)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, int64(1), affected)
		})

		t.Run("fail on second insert", func(t *testing.T) {
			// arrange
			var (
				conn, schema         = newSchema(t)
				streamType           = testdata.StreamType()
				streamID             = testdata.StreamID()
				storeStreamID        = testdata.StoreStreamID()
				eventNumber   int64  = 3
				watermark     int64  = 3
				partition     uint32 = 5
			)

			if _, err := schema.InsertOutbox(t.Context(), conn, streamType, streamID, storeStreamID, eventNumber, watermark, partition); err != nil {
				t.Fatal(err)
			}

			// act
			affected, err := schema.InsertOutbox(t.Context(), conn, streamType, streamID, storeStreamID, eventNumber, watermark, partition)

			// assert
			assert.Error(t, err)
			assert.Equal(t, int64(0), affected)
		})
	})

	t.Run("UpdateOutbox", func(t *testing.T) {
		t.Run("succeed update", func(t *testing.T) {
			// arrange
			var (
				conn, schema         = newSchema(t)
				streamType           = testdata.StreamType()
				streamID             = testdata.StreamID()
				storeStreamID        = testdata.StoreStreamID()
				eventNumber   int64  = 3
				watermark     int64  = 3
				partition     uint32 = 5
			)

			if _, err := schema.InsertOutbox(t.Context(), conn, streamType, streamID, storeStreamID, eventNumber, watermark, partition); err != nil {
				t.Fatal(err)
			}

			// act
			affected, err := schema.UpdateOutbox(t.Context(), conn, streamType, streamID, eventNumber+1, eventNumber)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, int64(1), affected)
		})

		t.Run("affect nothing on empty stream", func(t *testing.T) {
			// arrange
			var (
				conn, schema       = newSchema(t)
				streamType         = testdata.StreamType()
				streamID           = testdata.StreamID()
				eventNumber  int64 = 3
			)

			// act
			affected, err := schema.UpdateOutbox(t.Context(), conn, streamType, streamID, eventNumber, 0)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, int64(0), affected)
		})

		t.Run("affect nothing on wrong last event number", func(t *testing.T) {
			// arrange
			var (
				conn, schema            = newSchema(t)
				streamType              = testdata.StreamType()
				streamID                = testdata.StreamID()
				storeStreamID           = testdata.StoreStreamID()
				eventNumber      int64  = 3
				watermark        int64  = 3
				partition        uint32 = 5
				wrongEventNumber        = eventNumber + 42
			)

			if _, err := schema.InsertOutbox(t.Context(), conn, streamType, streamID, storeStreamID, eventNumber, watermark, partition); err != nil {
				t.Fatal(err)
			}

			// act
			affected, err := schema.UpdateOutbox(t.Context(), conn, streamType, streamID, eventNumber+1, wrongEventNumber)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, int64(0), affected)
		})
	})

	t.Run("SelectStreamIDs", func(t *testing.T) {
		t.Run("full list on empty token", func(t *testing.T) {
			// arrange
			var (
				conn, schema       = newSchema(t)
				streamType         = testdata.StreamType()
				count              = 10
				refs               = testdata.StreamReferences(streamType, count)
				token              = ""
				limit        int64 = 100
			)

			for _, ref := range refs {
				if _, err := schema.InsertOutbox(t.Context(), conn, streamType, ref.StreamID, ref.StoreStreamID, 1, 1, 1); err != nil {
					t.Fatal(err)
				}
			}

			// act
			got, err := schema.SelectStreamReferences(t.Context(), conn, streamType, token, limit)

			// assert
			assert.NoError(t, err)
			assertEqualRefsInRows(t, refs, got)
		})

		t.Run("read until limit", func(t *testing.T) {
			// arrange
			var (
				conn, schema       = newSchema(t)
				streamType         = testdata.StreamType()
				count              = 10
				refs               = testdata.StreamReferences(streamType, count)
				token              = ""
				limit        int64 = 5
			)

			for _, ref := range refs {
				if _, err := schema.InsertOutbox(t.Context(), conn, streamType, ref.StreamID, ref.StoreStreamID, 1, 1, 1); err != nil {
					t.Fatal(err)
				}
			}

			// act
			got, err := schema.SelectStreamReferences(t.Context(), conn, streamType, token, limit)

			// assert
			assert.NoError(t, err)
			assertEqualRefsInRows(t, refs[:limit], got)
		})

		t.Run("read from offset", func(t *testing.T) {
			// arrange
			var (
				conn, schema       = newSchema(t)
				streamType         = testdata.StreamType()
				count              = 10
				refs               = testdata.StreamReferences(streamType, count)
				offset             = 4
				token              = refs[offset-1].StoreStreamID
				limit        int64 = 100
			)

			for _, ref := range refs {
				if _, err := schema.InsertOutbox(t.Context(), conn, streamType, ref.StreamID, ref.StoreStreamID, 1, 1, 1); err != nil {
					t.Fatal(err)
				}
			}

			// act
			got, err := schema.SelectStreamReferences(t.Context(), conn, streamType, token, limit)

			// assert
			assert.NoError(t, err)
			assertEqualRefsInRows(t, refs[offset:], got)
		})

		t.Run("read nothing when offset after tail", func(t *testing.T) {
			// arrange
			var (
				conn, schema       = newSchema(t)
				streamType         = testdata.StreamType()
				count              = 10
				refs               = testdata.StreamReferences(streamType, count)
				token              = refs[count-1].StoreStreamID
				limit        int64 = 100
			)

			for _, ref := range refs {
				if _, err := schema.InsertOutbox(t.Context(), conn, streamType, ref.StreamID, ref.StoreStreamID, 1, 1, 1); err != nil {
					t.Fatal(err)
				}
			}

			// act
			got, err := schema.SelectStreamReferences(t.Context(), conn, streamType, token, limit)

			// assert
			assert.NoError(t, err)
			assertEqualRefsInRows(t, nil, got)
		})
	})

	t.Run("SelectOutboxStreamIDs", func(t *testing.T) {
		t.Run("read streams that have lower watermark", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				count        = 10
				streams      = testdata.StreamReferences(streamType, count)
				token        = ""
				grace        = time.Millisecond * 0
				partitions   = newPartitions(1)
				limit        = 100
			)

			insertOutbox(t, conn, schema, streams, 2, 1, 1)

			// act
			got, err := schema.SelectOutboxStreamIDs(t.Context(), conn, grace, partitions, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, streams, got)
		})

		t.Run("ignore streams that have watermark equal to event number", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				count        = 10
				streams      = testdata.StreamReferences(streamType, count)
				ignored      = testdata.StreamReferences(streamType, count)
				token        = ""
				grace        = time.Millisecond * 0
				partitions   = newPartitions(1)
				limit        = 100
			)

			insertOutbox(t, conn, schema, streams, 2, 1, 1)
			insertOutbox(t, conn, schema, ignored, 2, 2, 1)

			// act
			got, err := schema.SelectOutboxStreamIDs(t.Context(), conn, grace, partitions, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, streams, got)
		})

		t.Run("ignore streams that have wrong partition", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				count        = 10
				streams      = testdata.StreamReferences(streamType, count)
				ignored      = testdata.StreamReferences(streamType, count)
				token        = ""
				grace        = time.Millisecond * 0
				partitions   = newPartitions(1, 2, 3)
				limit        = 100
			)

			insertOutbox(t, conn, schema, streams, 2, 1, 1)
			insertOutbox(t, conn, schema, ignored, 2, 2, 42)

			// act
			got, err := schema.SelectOutboxStreamIDs(t.Context(), conn, grace, partitions, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, streams, got)
		})

		t.Run("read until limit", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				count        = 10
				streams      = testdata.StreamReferences(streamType, count)
				token        = ""
				grace        = time.Millisecond * 0
				partitions   = newPartitions(1)
				limit        = 5
			)

			insertOutbox(t, conn, schema, streams, 2, 1, 1)

			// act
			got, err := schema.SelectOutboxStreamIDs(t.Context(), conn, grace, partitions, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, streams[:limit], got)
		})

		t.Run("read from token", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				count        = 10
				streams      = testdata.StreamReferences(streamType, count)
				start        = 5
				token        = streams[start-1].StoreStreamID
				grace        = time.Millisecond * 0
				partitions   = newPartitions(1)
				limit        = 100
			)

			insertOutbox(t, conn, schema, streams, 2, 1, 1)

			// act
			got, err := schema.SelectOutboxStreamIDs(t.Context(), conn, grace, partitions, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, streams[start:], got)
		})

		t.Run("allow a grace period", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = testdata.StreamType()
				count        = 10
				streams      = testdata.StreamReferences(streamType, count)
				start        = 5
				token        = ""
				grace        = time.Millisecond * 100
				partitions   = newPartitions(1)
				limit        = 100
			)

			insertOutbox(t, conn, schema, streams, 2, 1, 1)
			for i, stream := range streams {
				var delay time.Duration = 0
				if i >= start {
					delay = -grace * 10
				}
				err := schema.UpdateOutboxWatermark(t.Context(), conn, stream, delay, database.OutboxWatermark{
					Watermark:  1,
					RetryCount: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
			}

			// act
			got, err := schema.SelectOutboxStreamIDs(t.Context(), conn, grace, partitions, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, streams[start:], got)
		})
	})

	t.Run("SelectOutboxWatermark", func(t *testing.T) {
		t.Run("read watermark", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				stream       = testdata.StreamReference()
			)

			_, err := schema.InsertOutbox(t.Context(), conn, stream.StreamType, stream.StreamID, stream.StoreStreamID, 300, 1, 42)
			assert.NoError(t, err)
			assert.NoError(t, schema.UpdateOutboxWatermark(t.Context(), conn, stream, 0, database.OutboxWatermark{
				Watermark:  1,
				RetryCount: 20,
			}))

			// act
			watermark, eventNumber, err := schema.SelectOutboxWatermark(t.Context(), conn, stream)

			// assert
			assert.NoError(t, err)
			assert.Equalf(t, int64(1), watermark.Watermark, "Watermark")
			assert.Equalf(t, int64(20), watermark.RetryCount, "RetryCount")
			assert.Equalf(t, int64(300), eventNumber, "eventNumber")
			assert.Equalf(t, stream.StreamID, watermark.StreamID, "StreamID")
		})

		t.Run("fail with no stream", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				stream       = testdata.StreamReference()
			)
			// act
			_, _, err := schema.SelectOutboxWatermark(t.Context(), conn, stream)

			// assert
			assert.Error(t, err)
		})
	})

	t.Run("UpdateOutboxWatermark", func(t *testing.T) {
		t.Run("update existing stream", func(t *testing.T) {
			// arrange
			var (
				conn, schema  = newSchema(t)
				streamType    = testdata.StreamType()
				streamID      = testdata.StreamID()
				storeStreamID = testdata.StoreStreamID()
				stream        = testdata.StreamReference(func(ref *es.StreamReference) {
					ref.StreamType = streamType
					ref.StreamID = storeStreamID
				})
				delay = time.Millisecond
			)

			_, err := schema.InsertOutbox(t.Context(), conn, stream.StreamType, streamID, stream.StoreStreamID, 300, 1, 42)
			assert.NoError(t, err)

			// act
			err = schema.UpdateOutboxWatermark(t.Context(), conn, stream, delay, database.OutboxWatermark{
				Watermark:  10,
				RetryCount: 20,
			})

			// assert
			assert.NoError(t, err)
			got, _, err := schema.SelectOutboxWatermark(t.Context(), conn, stream)
			assert.NoError(t, err)
			assert.Equalf(t, 10, got.Watermark, "Watermark")
			assert.Equalf(t, 20, got.RetryCount, "RetryCount")
		})

		t.Run("fail on missing stream", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				stream       = testdata.StreamReference()
				delay        = time.Millisecond
			)

			// act
			err := schema.UpdateOutboxWatermark(t.Context(), conn, stream, delay, database.OutboxWatermark{
				Watermark:  10,
				RetryCount: 20,
			})

			// assert
			assert.Error(t, err)
		})
	})
}
