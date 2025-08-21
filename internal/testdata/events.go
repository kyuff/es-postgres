package testdata

import (
	"math/rand/v2"
	"time"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/uuid"
)

type MockEvent struct {
	ID int `json:"id"`
}

func (e MockEvent) EventName() string {
	return "MockEvent"
}

func StreamID() string {
	return uuid.V7()
}

func StreamIDs(n int) []string {
	var ids []string
	for range n {
		ids = append(ids, StreamID())
	}
	return ids
}

func StreamType() string {
	return uuid.V7()
}
func StoreEventID() string {
	return uuid.V7()
}
func StoreStreamID() string {
	return uuid.V7()
}

func Event(eventNumber int64, mods ...func(e *es.Event)) es.Event {
	e := es.Event{
		StreamID:      StreamID(),
		StreamType:    StreamType(),
		EventNumber:   eventNumber,
		EventTime:     time.Now().Add(time.Second * time.Duration(eventNumber)).Truncate(time.Second),
		Content:       MockEvent{ID: rand.IntN(1000)},
		StoreEventID:  StoreEventID(),
		StoreStreamID: StoreStreamID(),
	}
	for _, mod := range mods {
		mod(&e)
	}

	return e
}
func Events(count int, mods ...func(e *es.Event)) []es.Event {
	var (
		streamType    = StreamType()
		streamID      = StreamID()
		storeStreamID = StoreStreamID()
	)
	var events []es.Event
	var modifications []func(e *es.Event)
	modifications = append(modifications, func(e *es.Event) {
		e.StreamType = streamType
		e.StreamID = streamID
		e.StoreStreamID = storeStreamID
	})
	modifications = append(modifications, mods...)
	for i := 1; i <= count; i++ {

		events = append(events, Event(int64(i), modifications...))
	}

	return events
}
