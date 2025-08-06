package postgres

import (
	"errors"
	"fmt"
	"iter"
	"testing"
	"time"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/seqs"
	"github.com/kyuff/es-postgres/internal/uuid"
)

type MockEvent struct {
}

func (e MockEvent) EventName() string {
	return "MockEvent"
}

func TestValidateStreamWrite(t *testing.T) {
	var (
		newStreamType    = uuid.V7
		newStreamID      = uuid.V7
		newStoreStreamID = uuid.V7
		newEvent         = func(eventNumber int64, mods ...func(e *es.Event)) es.Event {
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
		newEvents = func(streamType, streamID string, storeStreamID string, count int) []es.Event {
			var events []es.Event
			var storeEventIDs = uuid.V7At(time.Now(), count)
			for i := 1; i <= count; i++ {
				events = append(events, newEvent(int64(i), func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
					e.StoreEventID = storeEventIDs[i-1]
					e.StoreStreamID = storeStreamID
					e.Content = MockEvent{}
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
		assertHasError = func(t *testing.T, events iter.Seq2[es.Event, error]) bool {
			for _, err := range events {
				if err != nil {
					return true
				}
			}
			t.Logf("Expected an error in iter.Seq2")
			t.Fail()
			return false
		}
	)
	_ = eventsEqual
	_ = newEvents
	_ = newEvent

	t.Run("fail on error", func(t *testing.T) {
		// arrange
		var (
			streamType    = newStreamType()
			streamID      = newStreamID()
			storeStreamID = newStoreStreamID()
			events        = seqs.Concat2(
				seqs.Error2[es.Event](errors.New("TEST")),
				seqs.Seq2(newEvents(streamType, streamID, storeStreamID, 3)...),
			)
		)

		// act
		got := validateStreamWrite(streamType, events)

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on stream type mismatch", func(t *testing.T) {
		// arrange
		var (
			streamTypeA   = newStreamType()
			streamTypeB   = newStreamType()
			streamID      = newStreamID()
			storeStreamID = newStoreStreamID()
			events        = seqs.Concat2(
				seqs.Seq2(newEvents(streamTypeA, streamID, storeStreamID, 3)...),
				seqs.Seq2(newEvents(streamTypeB, streamID, storeStreamID, 3)...),
			)
		)

		// act
		got := validateStreamWrite(streamTypeA, events)

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on stream id mismatch", func(t *testing.T) {
		// arrange
		var (
			streamType    = newStreamType()
			streamIDA     = newStreamID()
			streamIDB     = newStreamID()
			storeStreamID = newStoreStreamID()
			events        = seqs.Concat2(
				seqs.Seq2(newEvents(streamType, streamIDA, storeStreamID, 3)...),
				seqs.Seq2(newEvents(streamType, streamIDB, storeStreamID, 3)...),
			)
		)

		// act
		got := validateStreamWrite(streamType, events)

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on store stream id mismatch", func(t *testing.T) {
		// arrange
		var (
			streamType     = newStreamType()
			streamID       = newStreamID()
			storeStreamIDA = newStoreStreamID()
			storeStreamIDB = newStoreStreamID()
			events         = seqs.Concat2(
				seqs.Seq2(newEvents(streamType, streamID, storeStreamIDA, 3)...),
				seqs.Seq2(newEvents(streamType, streamID, storeStreamIDB, 3)...),
			)
		)

		// act
		got := validateStreamWrite(streamType, events)

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on double event number", func(t *testing.T) {
		// arrange
		var (
			streamType    = newStreamType()
			streamID      = newStreamID()
			storeStreamID = newStoreStreamID()
			events        = newEvents(streamType, streamID, storeStreamID, 3)
		)

		// act
		got := validateStreamWrite(streamType, seqs.Concat2(
			seqs.Seq2(events...),
			seqs.Seq2(events[2:]...),
		))

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on missing event number", func(t *testing.T) {
		// arrange
		var (
			streamType    = newStreamType()
			streamID      = newStreamID()
			storeStreamID = newStoreStreamID()
			events        = newEvents(streamType, streamID, storeStreamID, 10)
		)

		// act
		got := validateStreamWrite(streamType, seqs.Concat2(
			seqs.Seq2(events[0:2]...),
			seqs.Seq2(events[5:]...),
		))

		// assert
		assertHasError(t, got)
	})

	t.Run("validate full sequence", func(t *testing.T) {
		// arrange
		var (
			streamType    = newStreamType()
			streamID      = newStreamID()
			storeStreamID = newStoreStreamID()
			events        = newEvents(streamType, streamID, storeStreamID, 10)
		)

		// act
		got := validateStreamWrite(streamType, seqs.Seq2(events...))

		// assert
		assert.EqualSeq2(t, seqs.Seq2(events...), got, eventsEqual(t))
	})
}
