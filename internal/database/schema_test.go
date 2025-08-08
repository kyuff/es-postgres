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
		newSchema        = func(t *testing.T) (*pgxpool.Pool, *database.Schema) {
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
	)

	t.Run("return current migration", func(t *testing.T) {
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

	t.Run("write event successfully", func(t *testing.T) {
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

	t.Run("write event fails due to duplicate", func(t *testing.T) {
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

	t.Run("select all events successfully", func(t *testing.T) {
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

	t.Run("select event tail successfully", func(t *testing.T) {
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

	t.Run("select empty event stream", func(t *testing.T) {
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

	t.Run("insert outbox successfully", func(t *testing.T) {
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

	t.Run("insert outbox fails on second insert", func(t *testing.T) {
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

	t.Run("update outbox succeeds", func(t *testing.T) {
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

	t.Run("update outbox affects nothing on no stream", func(t *testing.T) {
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

	t.Run("update outbox affects nothing on wrong last event number", func(t *testing.T) {
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

}
