package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	postgres "github.com/kyuff/es-postgres"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/seqs"
	"github.com/kyuff/es-postgres/internal/testdata"
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

var newInstance = func(t *testing.T, opts ...postgres.Option) *postgres.Storage {
	t.Helper()
	storage, err := postgres.New(postgres.InstanceFromDSN(database.DSNTest(t)), opts...)
	if err != nil {
		t.Logf("Failed creating storage: %s", err)
		t.FailNow()
	}

	t.Cleanup(func() {
		_ = storage.Close()
	})

	return storage
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
				Content:      EventA{A: rand.Int()},
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
	t.Run("Instance", func(t *testing.T) {
		t.Parallel()
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
	})

	t.Run("Register", func(t *testing.T) {
		t.Parallel()
		t.Run("should register event types", func(t *testing.T) {
			// arrange
			var (
				storage    = newInstance(t)
				streamType = newStreamType()
			)

			// act
			err := storage.Register(streamType, EventA{}, EventB{})

			// assert
			assert.NoError(t, err)
		})
	})

	t.Run("Read/Write", func(t *testing.T) {
		t.Parallel()
		t.Run("should read no events", func(t *testing.T) {
			// arrange
			var (
				storage    = newInstance(t)
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
				storage    = newInstance(t)
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
				storage    = newInstance(t)
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
				storage    = newInstance(t)
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
	})

	t.Run("GetStreamReferences", func(t *testing.T) {
		t.Parallel()
		t.Run("should read a list of stream ids", func(t *testing.T) {
			// arrange
			var (
				storage    = newInstance(t)
				streamType = newStreamType()
				count      = 10
				refs       = testdata.StreamReferences(streamType, count)
			)

			assert.NoError(t, storage.Register(streamType, EventA{}, EventB{}))

			for i := range count {
				assert.NoError(t, storage.Write(t.Context(), streamType, seqs.Seq2(newEvent(1, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = refs[i].StreamID
					e.StoreStreamID = refs[i].StoreStreamID
				}))))
			}

			// act
			got := storage.GetStreamReferences(t.Context(), streamType, "", 1000)

			// assert
			assert.EqualSeq2(t, seqs.Seq2(refs...), got, func(expected, got assert.KeyValue[es.StreamReference, error]) bool {
				return assert.Equal(t, expected.Key, got.Key)
			})
		})

		t.Run("should read a list of stream ids filtered by token", func(t *testing.T) {
			// arrange
			var (
				storage    = newInstance(t)
				streamType = newStreamType()
				count      = 10
				refs       = testdata.StreamReferences(streamType, count)
			)

			assert.NoError(t, storage.Register(streamType, EventA{}, EventB{}))

			for i := range count {
				assert.NoError(t, storage.Write(t.Context(), streamType, seqs.Seq2(newEvent(1, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = refs[i].StreamID
					e.StoreStreamID = refs[i].StoreStreamID
				}))))
			}

			// act
			got := storage.GetStreamReferences(t.Context(), streamType, refs[4].StoreStreamID, 1000)

			// assert
			assert.EqualSeq2(t, seqs.Seq2(refs[5:]...), got, func(expected, got assert.KeyValue[es.StreamReference, error]) bool {
				return assert.Equal(t, expected.Key, got.Key)
			})
		})

		t.Run("should return next token when reading list of stream ids", func(t *testing.T) {
			// arrange
			var (
				storage    = newInstance(t)
				streamType = newStreamType()
				count      = 10
				refs       = testdata.StreamReferences(streamType, count)
			)

			assert.NoError(t, storage.Register(streamType, EventA{}, EventB{}))

			for i := range count {
				assert.NoError(t, storage.Write(t.Context(), streamType, seqs.Seq2(newEvent(1, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = refs[i].StreamID
					e.StoreStreamID = refs[i].StoreStreamID
				}))))
			}

			// act
			got := storage.GetStreamReferences(t.Context(), streamType, "", 4)

			// assert
			assert.EqualSeq2(t, seqs.Seq2(refs[:4]...), got, func(expected, got assert.KeyValue[es.StreamReference, error]) bool {
				return assert.Equal(t, expected.Key, got.Key)
			})
		})

		t.Run("should return empty token when outside range", func(t *testing.T) {
			// arrange
			var (
				storage    = newInstance(t)
				streamType = newStreamType()
				count      = 10
				token      = uuid.V7AtTime(time.Now().Add(time.Hour))
				refs       = testdata.StreamReferences(streamType, count)
			)

			assert.NoError(t, storage.Register(streamType, EventA{}, EventB{}))

			for i := range count {
				assert.NoError(t, storage.Write(t.Context(), streamType, seqs.Seq2(newEvent(1, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = refs[i].StreamID
					e.StoreStreamID = refs[i].StoreStreamID
				}))))
			}

			// act
			got := storage.GetStreamReferences(t.Context(), streamType, token, 4)

			// assert
			assert.EqualSeq2(t, seqs.EmptySeq2[es.StreamReference, error](), got, func(expected, got assert.KeyValue[es.StreamReference, error]) bool {
				return assert.Equal(t, expected.Key, got.Key)
			})
		})
	})

	t.Run("StartPublish", func(t *testing.T) {
		t.Parallel()
		t.Run("write all events", func(t *testing.T) {
			t.Parallel()
			// arrange
			var (
				storage = newInstance(t,
					postgres.WithReconcileInterval(time.Millisecond*100),
					postgres.WithLeaseHeartbeatInterval(time.Millisecond*100),
				)
				w = &WriterMock{}

				streamType = newStreamType()
				streamID   = newStreamID()
				events     = newEvents(streamType, streamID, 10)
				got        []es.Event
				mu         sync.RWMutex
			)

			w.WriteFunc = func(ctx context.Context, typ string, eventSeq iter.Seq2[es.Event, error]) error {
				mu.Lock()
				defer mu.Unlock()

				assert.Equalf(t, streamType, typ, "stream type mismatch")
				for event, err := range eventSeq {
					if assert.NoError(t, err) {
						got = append(got, event)
					}
				}
				return nil
			}

			assert.NoError(t, storage.Register(streamType, EventA{}, EventB{}))

			go func() {
				_ = storage.StartPublish(t.Context(), w)
			}()

			// act
			err := storage.Write(t.Context(), streamType, seqs.Seq2(events...))

			// assert
			assert.NoError(t, err)
			assert.NoErrorEventually(t, time.Second*5, func() error {
				mu.RLock()
				defer mu.RUnlock()

				if len(got) < 10 {
					return errors.New("no events received")
				}
				return nil
			})
			assert.EqualSlice(t, events, got)
			assert.Equal(t, 1, len(w.WriteCalls()))
		})

		t.Run("repeat write on error", func(t *testing.T) {
			t.Parallel()
			// arrange
			var (
				storage = newInstance(t,
					postgres.WithReconcileInterval(time.Millisecond*50),
					postgres.WithFixedProcessBackoff(time.Millisecond*10),
					postgres.WithDefaultSlog(),
				)
				w = &WriterMock{}

				streamType = newStreamType()
				streamID   = newStreamID()
				events     = newEvents(streamType, streamID, 10)
				got        []es.Event
				writes     = 0
				mu         sync.RWMutex
			)

			w.WriteFunc = func(ctx context.Context, typ string, eventSeq iter.Seq2[es.Event, error]) error {
				mu.Lock()
				defer mu.Unlock()

				writes++
				if writes < 3 {
					return errors.New("test write error")
				}

				assert.Equalf(t, streamType, typ, "stream type mismatch")
				for event, err := range eventSeq {
					if assert.NoError(t, err) {
						got = append(got, event)
					}
				}
				return nil
			}

			assert.NoError(t, storage.Register(streamType, EventA{}, EventB{}))

			go func() {
				_ = storage.StartPublish(t.Context(), w)
			}()

			// act
			err := storage.Write(t.Context(), streamType, seqs.Seq2(events...))

			// assert
			assert.NoError(t, err)
			assert.NoErrorEventually(t, time.Second*5, func() error {
				mu.RLock()
				defer mu.RUnlock()

				if len(got) < 10 {
					return errors.New("no events received")
				}
				return nil
			})
			assert.EqualSlice(t, events, got)
			assert.Equal(t, 3, len(w.WriteCalls()))
		})
	})
}
