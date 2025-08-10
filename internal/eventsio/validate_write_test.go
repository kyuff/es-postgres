package eventsio_test

import (
	"errors"
	"iter"
	"testing"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/eventsio"
	"github.com/kyuff/es-postgres/internal/seqs"
	"github.com/kyuff/es-postgres/internal/testdata"
)

type MockEvent struct {
}

func (e MockEvent) EventName() string {
	return "MockEvent"
}

func TestValidateWrite(t *testing.T) {
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

	t.Run("fail on error", func(t *testing.T) {
		// arrange
		var (
			streamType = testdata.StreamType()
			events     = seqs.Concat2(
				seqs.Error2[es.Event](errors.New("TEST")),
				seqs.Seq2(testdata.Events(3, func(e *es.Event) {
					e.StreamType = streamType
				})...),
			)
			sut = eventsio.NewValidator()
		)

		// act
		got := sut.Validate(streamType, events)

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on stream type mismatch", func(t *testing.T) {
		// arrange
		var (
			streamTypeA = testdata.StreamType()
			streamTypeB = testdata.StreamType()
			events      = seqs.Concat2(
				seqs.Seq2(testdata.Events(3, func(e *es.Event) {
					e.StreamType = streamTypeA
				})...),
				seqs.Seq2(testdata.Events(3, func(e *es.Event) {
					e.StreamType = streamTypeB
				})...),
			)
			sut = eventsio.NewValidator()
		)

		// act
		got := sut.Validate(streamTypeA, events)

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on stream id mismatch", func(t *testing.T) {
		// arrange
		var (
			streamType = testdata.StreamType()
			streamIDA  = testdata.StreamID()
			streamIDB  = testdata.StreamID()
			events     = seqs.Concat2(
				seqs.Seq2(testdata.Events(3, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamIDA
				})...),
				seqs.Seq2(testdata.Events(3, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamIDB
				})...),
			)
			sut = eventsio.NewValidator()
		)

		// act
		got := sut.Validate(streamType, events)

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on store stream id mismatch", func(t *testing.T) {
		// arrange
		var (
			streamType     = testdata.StreamType()
			streamID       = testdata.StreamID()
			storeStreamIDA = testdata.StoreStreamID()
			storeStreamIDB = testdata.StoreStreamID()
			events         = seqs.Concat2(
				seqs.Seq2(testdata.Events(3, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
					e.StoreStreamID = storeStreamIDA
				})...),
				seqs.Seq2(testdata.Events(3, func(e *es.Event) {
					e.StreamType = streamType
					e.StreamID = streamID
					e.StoreStreamID = storeStreamIDB
				})...),
			)
			sut = eventsio.NewValidator()
		)

		// act
		got := sut.Validate(streamType, events)

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on double event number", func(t *testing.T) {
		// arrange
		var (
			streamType = testdata.StreamType()
			events     = testdata.Events(3, func(e *es.Event) {
				e.StreamType = streamType
			})
			sut = eventsio.NewValidator()
		)

		// act
		got := sut.Validate(streamType, seqs.Concat2(
			seqs.Seq2(events...),
			seqs.Seq2(events[2:]...),
		))

		// assert
		assertHasError(t, got)
	})

	t.Run("fail on missing event number", func(t *testing.T) {
		// arrange
		var (
			streamType = testdata.StreamType()
			events     = testdata.Events(10, func(e *es.Event) {
				e.StreamType = streamType
			})
			sut = eventsio.NewValidator()
		)

		// act
		got := sut.Validate(streamType, seqs.Concat2(
			seqs.Seq2(events[0:2]...),
			seqs.Seq2(events[5:]...),
		))

		// assert
		assertHasError(t, got)
	})

	t.Run("validate full sequence", func(t *testing.T) {
		// arrange
		var (
			streamType = testdata.StreamType()
			events     = testdata.Events(10, func(e *es.Event) {
				e.StreamType = streamType
			})
			sut = eventsio.NewValidator()
		)

		// act
		got := sut.Validate(streamType, seqs.Seq2(events...))

		// assert
		assert.EqualSeq2(t, seqs.Seq2(events...), got, eventsEqual(t))
	})
}
