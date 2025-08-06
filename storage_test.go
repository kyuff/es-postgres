package postgres_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	postgres "github.com/kyuff/es-postgres"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/seqs"
	"github.com/kyuff/es-postgres/internal/uuid"
)

type EventA struct {
	A int `json:"a"`
}

func (e EventA) EventName() string {
	return "EventA"
}

type EventB struct {
	B string `json:"b"`
}

func (e EventB) EventName() string {
	return "EventB"
}

func TestStorage(t *testing.T) {
	var (
		newStreamType = uuid.V7
		newStreamID   = uuid.V7
		newEvent      = func(eventNumber int64, mods ...func(e *es.Event)) es.Event {
			e := es.Event{
				StreamID:     fmt.Sprintf("StreamID-%d", eventNumber),
				StreamType:   fmt.Sprintf("StreamType-%d", eventNumber),
				EventNumber:  eventNumber,
				StoreEventID: uuid.V7(),
				EventTime:    time.Now().Add(time.Second * time.Duration(eventNumber)).Truncate(time.Second),
			}
			for _, mod := range mods {
				mod(&e)
			}

			return e
		}
		newEvents = func(streamType, streamID string, count int) []es.Event {
			var events []es.Event
			var storeEventIDs = uuid.V7At(time.Now(), count)
			var storeStreamID = uuid.V7()
			for i := 1; i <= count; i++ {
				var content es.Content
				if i%2 == 0 {
					content = EventA{A: rand.Intn(100)}
				} else {
					content = EventB{B: uuid.V7()}
				}
				events = append(events, newEvent(int64(i), func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
					e.StoreEventID = storeEventIDs[i-1]
					e.StoreStreamID = storeStreamID
					e.Content = content
				}))
			}

			return events
		}
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
	)
	t.Run("should create a new storage from dsn", func(t *testing.T) {
		// arrange
		var (
			connector = postgres.InstanceFromDSN(database.DSNTest(t))
		)

		// act
		storage, err := postgres.New(connector)

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, storage)
		assert.NoError(t, storage.Close())
	})

	t.Run("should create a new storage from pool", func(t *testing.T) {
		// arrange
		var (
			dsn  = database.DSNTest(t)
			pool = assert.MustNoError(t, func() (*pgxpool.Pool, error) {
				return database.Connect(t.Context(), dsn)
			})
			connector = postgres.InstanceFromPool(pool)
		)

		// act
		storage, err := postgres.New(connector)

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, storage)
		assert.NoError(t, storage.Close())
	})

	t.Run("should register event types", func(t *testing.T) {
		// arrange
		var (
			storage = assert.MustNoError(t, func() (*postgres.Storage, error) {
				return postgres.New(postgres.InstanceFromDSN(database.DSNTest(t)))
			})
			streamType = newStreamType()
		)

		// act
		err := storage.Register(streamType, EventA{}, EventB{})

		// assert
		assert.NoError(t, err)
	})

	t.Run("should read no events", func(t *testing.T) {
		// arrange
		var (
			storage = assert.MustNoError(t, func() (*postgres.Storage, error) {
				return postgres.New(postgres.InstanceFromDSN(database.DSNTest(t)))
			})
			streamType = newStreamType()
			streamID   = newStreamID()
		)

		// act
		got := storage.Read(t.Context(), streamType, streamID, 0)

		// assert
		assert.EqualSeq2(t, seqs.EmptySeq2[es.Event, error](), got, func(expected, got assert.KeyValue[es.Event, error]) bool {
			return false
		})
	})

	t.Run("should write and read events", func(t *testing.T) {
		// arrange
		var (
			storage = assert.MustNoError(t, func() (*postgres.Storage, error) {
				return postgres.New(postgres.InstanceFromDSN(database.DSNTest(t)))
			})
			streamType = newStreamType()
			streamID   = newStreamID()
			events     = newEvents(streamType, streamID, 10)
		)

		assert.NoError(t, storage.Register(streamType, EventA{}, EventB{}))
		assert.NoError(t, storage.Write(t.Context(), streamType, seqs.Seq2(events...)))

		// act
		got := storage.Read(t.Context(), streamType, streamID, 0)

		// assert
		assert.EqualSeq2(t, seqs.Seq2(events...), got, eventsEqual(t))
	})

	t.Run("should support optimistic locks by failing if event_number already written", func(t *testing.T) {
		// arrange
		var (
			storage = assert.MustNoError(t, func() (*postgres.Storage, error) {
				return postgres.New(postgres.InstanceFromDSN(database.DSNTest(t)))
			})
			streamType = newStreamType()
			streamID   = newStreamID()
			events     = newEvents(streamType, streamID, 10)
		)

		assert.NoError(t, storage.Register(streamType, EventA{}, EventB{}))
		assert.NoError(t, storage.Write(t.Context(), streamType, seqs.Seq2(events[0:5]...)))

		// act
		err := storage.Write(t.Context(), streamType, seqs.Seq2(events[2:]...))

		// assert
		assert.Error(t, err)
		assert.EqualSeq2(t,
			seqs.Seq2(events[0:5]...),
			storage.Read(t.Context(), streamType, streamID, 0),
			eventsEqual(t))
	})

	t.Run("should support optimistic locks by failing if event_number too high", func(t *testing.T) {
		// arrange
		var (
			storage = assert.MustNoError(t, func() (*postgres.Storage, error) {
				return postgres.New(postgres.InstanceFromDSN(database.DSNTest(t)))
			})
			streamType = newStreamType()
			streamID   = newStreamID()
			events     = newEvents(streamType, streamID, 10)
		)

		assert.NoError(t, storage.Register(streamType, EventA{}, EventB{}))
		assert.NoError(t, storage.Write(t.Context(), streamType, seqs.Seq2(events[0:3]...)))

		// act
		err := storage.Write(t.Context(), streamType, seqs.Seq2(events[7:]...))

		// assert
		assert.Error(t, err)
		assert.EqualSeq2(t,
			seqs.Seq2(events[0:3]...),
			storage.Read(t.Context(), streamType, streamID, 0),
			eventsEqual(t))
	})
}
