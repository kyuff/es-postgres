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
	"github.com/kyuff/es-postgres/internal/uuid"
)

type MockEvent struct {
}

func (MockEvent) EventName() string {
	return "MockEvent"
}

func TestSchema(t *testing.T) {
	var (
		newStreamType    = uuid.V7
		newStreamID      = uuid.V7
		newStoreStreamID = uuid.V7
		newStreamIDs     = func(n int) []string {
			var ids []string
			for range n {
				ids = append(ids, newStreamID())
			}
			return ids
		}
		newPartitions = func(partitions ...uint32) []uint32 {
			return partitions
		}
		newStreams = func(streamType string, count int) []database.Stream {
			var s []database.Stream
			for _, storeStreamID := range uuid.V7At(time.Now(), count) {
				s = append(s, database.Stream{
					StoreID: storeStreamID,
					Type:    streamType,
				})
			}
			return s
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
		newEvent = func(eventNumber int64, mods ...func(e *es.Event)) es.Event {
			e := es.Event{
				StreamID:      uuid.V7(),
				StreamType:    uuid.V7(),
				EventNumber:   eventNumber,
				EventTime:     time.Now().Add(time.Second * time.Duration(eventNumber)).Truncate(time.Second),
				Content:       MockEvent{},
				StoreEventID:  uuid.V7(),
				StoreStreamID: uuid.V7(),
			}
			for _, mod := range mods {
				mod(&e)
			}

			return e
		}
		newEvents = func(streamType, streamID string, count int) []es.Event {
			var events []es.Event
			for i := 1; i <= count; i++ {
				events = append(events, newEvent(int64(i), func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
				}))
			}

			return events
		}
		writeEvents = func(t *testing.T, db database.DBTX, schema *database.Schema, events []es.Event) {
			for _, event := range events {
				assert.NoError(t, schema.WriteEvent(t.Context(), db, event))
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
		insertOutbox = func(t *testing.T, conn database.DBTX, schema *database.Schema, streams []database.Stream, eventNumber, watermark int64, partition uint32) {
			streamIDs := newStreamIDs(len(streams))
			for i, stream := range streams {
				if _, err := schema.InsertOutbox(t.Context(), conn, stream.Type, streamIDs[i], stream.StoreID, eventNumber, watermark, partition); err != nil {
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
				streamType   = newStreamType()
				streamID     = newStreamID()
				events       = newEvents(streamType, streamID, 1)
			)

			// act
			err := schema.WriteEvent(t.Context(), conn, events[0])

			// assert
			assert.NoError(t, err)
		})

		t.Run("fail due to duplicate", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = newStreamType()
				streamID     = newStreamID()
				events       = newEvents(streamType, streamID, 1)
			)

			assert.NoError(t, schema.WriteEvent(t.Context(), conn, events[0]))

			// act
			err := schema.WriteEvent(t.Context(), conn, events[0])

			// assert
			assert.Error(t, err)
		})
	})

	t.Run("SelectEvents", func(t *testing.T) {
		t.Run("full stream", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = newStreamType()
				streamID     = newStreamID()
				events       = newEvents(streamType, streamID, 10)
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
				streamType   = newStreamType()
				streamID     = newStreamID()
				events       = newEvents(streamType, streamID, 10)
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
				streamType   = newStreamType()
				streamID     = newStreamID()
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
				streamType           = newStreamType()
				streamID             = newStreamID()
				storeStreamID        = newStoreStreamID()
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
				streamType           = newStreamType()
				streamID             = newStreamID()
				storeStreamID        = newStoreStreamID()
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
				streamType           = newStreamType()
				streamID             = newStreamID()
				storeStreamID        = newStoreStreamID()
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
				streamType         = newStreamType()
				streamID           = newStreamID()
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
				streamType              = newStreamType()
				streamID                = newStreamID()
				storeStreamID           = newStoreStreamID()
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
				conn, schema         = newSchema(t)
				streamType           = newStreamType()
				count                = 10
				storeStreamIDs       = uuid.V7At(time.Now(), count)
				streamIDs            = newStreamIDs(count)
				token                = ""
				limit          int64 = 100
			)

			for i, id := range storeStreamIDs {
				if _, err := schema.InsertOutbox(t.Context(), conn, streamType, streamIDs[i], id, 1, 1, 1); err != nil {
					t.Fatal(err)
				}
			}

			// act
			got, nextToken, err := schema.SelectStreamIDs(t.Context(), conn, streamType, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, streamIDs, got)
			assert.Equal(t, storeStreamIDs[count-1], nextToken)
		})

		t.Run("read until limit", func(t *testing.T) {
			// arrange
			var (
				conn, schema         = newSchema(t)
				streamType           = newStreamType()
				count                = 10
				storeStreamIDs       = uuid.V7At(time.Now(), count)
				streamIDs            = newStreamIDs(count)
				token                = ""
				limit          int64 = 5
			)

			for i, id := range storeStreamIDs {
				if _, err := schema.InsertOutbox(t.Context(), conn, streamType, streamIDs[i], id, 1, 1, 1); err != nil {
					t.Fatal(err)
				}
			}

			// act
			got, nextToken, err := schema.SelectStreamIDs(t.Context(), conn, streamType, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, streamIDs[:limit], got)
			assert.Equal(t, storeStreamIDs[limit-1], nextToken)
		})

		t.Run("read from offset", func(t *testing.T) {
			// arrange
			var (
				conn, schema         = newSchema(t)
				streamType           = newStreamType()
				count                = 10
				storeStreamIDs       = uuid.V7At(time.Now(), count)
				streamIDs            = newStreamIDs(count)
				offset               = 4
				token                = storeStreamIDs[offset-1]
				limit          int64 = 100
			)

			for i, id := range storeStreamIDs {
				if _, err := schema.InsertOutbox(t.Context(), conn, streamType, streamIDs[i], id, 1, 1, 1); err != nil {
					t.Fatal(err)
				}
			}

			// act
			got, nextToken, err := schema.SelectStreamIDs(t.Context(), conn, streamType, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, streamIDs[offset:], got)
			assert.Equal(t, storeStreamIDs[count-1], nextToken)
		})

		t.Run("read nothing when offset after tail", func(t *testing.T) {
			// arrange
			var (
				conn, schema         = newSchema(t)
				streamType           = newStreamType()
				count                = 10
				storeStreamIDs       = uuid.V7At(time.Now(), count)
				streamIDs            = newStreamIDs(count)
				token                = storeStreamIDs[count-1]
				limit          int64 = 100
			)

			for i, id := range storeStreamIDs {
				if _, err := schema.InsertOutbox(t.Context(), conn, streamType, streamIDs[i], id, 1, 1, 1); err != nil {
					t.Fatal(err)
				}
			}

			// act
			got, nextToken, err := schema.SelectStreamIDs(t.Context(), conn, streamType, token, limit)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, nil, got)
			assert.Equal(t, token, nextToken)
		})
	})

	t.Run("SelectOutboxStreamIDs", func(t *testing.T) {
		t.Run("read streams that have lower watermark", func(t *testing.T) {
			// arrange
			var (
				conn, schema = newSchema(t)
				streamType   = newStreamType()
				count        = 10
				streams      = newStreams(streamType, count)
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
				streamType   = newStreamType()
				count        = 10
				streams      = newStreams(streamType, count)
				ignored      = newStreams(streamType, count)
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
				streamType   = newStreamType()
				count        = 10
				streams      = newStreams(streamType, count)
				ignored      = newStreams(streamType, count)
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
				streamType   = newStreamType()
				count        = 10
				streams      = newStreams(streamType, count)
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
				streamType   = newStreamType()
				count        = 10
				streams      = newStreams(streamType, count)
				start        = 5
				token        = streams[start-1].StoreID
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
				streamType   = newStreamType()
				count        = 10
				streams      = newStreams(streamType, count)
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

	})

	t.Run("UpdateOutboxWatermark", func(t *testing.T) {

	})
}
