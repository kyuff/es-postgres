package database_test

import (
	"fmt"
	"testing"
	"time"

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
				StreamID:     fmt.Sprintf("StreamID-%d", eventNumber),
				StreamType:   fmt.Sprintf("StreamType-%d", eventNumber),
				EventNumber:  eventNumber,
				StoreEventID: uuid.V7(),
				EventTime:    time.Now().Add(time.Second * time.Duration(eventNumber)).Truncate(time.Second),
				Content:      MockEvent{},
			}
			for _, mod := range mods {
				mod(&e)
			}

			return e
		}
		newEvents = func(stream database.Stream, count int) []es.Event {
			var events []es.Event
			var streamType = stream.Type
			var streamID = uuid.V7()
			var storeEventIDs = uuid.V7At(time.Now(), count)
			for i := 1; i <= count; i++ {
				events = append(events, newEvent(int64(i), func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
					e.StoreEventID = storeEventIDs[i-1]
					e.StoreStreamID = stream.StoreID
					e.Content = MockEvent{}
				}))
			}

			return events
		}
		newStream = func() database.Stream {
			return database.Stream{
				StoreID: uuid.V7(),
				Type:    uuid.V7(),
			}
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
			stream       = newStream()
			events       = newEvents(stream, 1)
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
			stream       = newStream()
			events       = newEvents(stream, 1)
		)

		assert.NoError(t, schema.WriteEvent(t.Context(), conn, events[0]))

		// act
		err := schema.WriteEvent(t.Context(), conn, events[0])

		// assert
		assert.Error(t, err)
	})
}
