package testdata

import (
	"time"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/uuid"
)

type MockEvent struct {
	ID int
}

func (e MockEvent) EventName() string {
	return "MockEvent"
}

func StreamID() string {
	return uuid.V7()
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
		Content:       MockEvent{},
		StoreEventID:  StoreEventID(),
		StoreStreamID: StoreStreamID(),
	}
	for _, mod := range mods {
		mod(&e)
	}

	return e
}
func Events(count int, mods ...func(e *es.Event)) []es.Event {
	var events []es.Event
	for i := 1; i <= count; i++ {
		events = append(events, Event(int64(i), mods...))
	}

	return events
}
