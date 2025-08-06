package postgres

import (
	"fmt"
	"iter"

	"github.com/kyuff/es"
)

func validateStreamWrite(streamType string, events iter.Seq2[es.Event, error]) iter.Seq2[es.Event, error] {
	return func(yield func(es.Event, error) bool) {
		var (
			first         = true
			storeStreamID string
			streamID      string
			eventNumber   int64
		)

		for event, err := range events {
			if err != nil {
				yield(event, err)
				return
			}

			if first {
				first = false
				storeStreamID = event.StoreStreamID
				streamID = event.StreamID
				eventNumber = event.EventNumber - 1
			}

			if event.StreamType != streamType {
				yield(event, fmt.Errorf("[es/postgres] Invalid stream type: %q", event.StreamType))
				return
			}

			if event.StoreStreamID != storeStreamID {
				yield(event, fmt.Errorf("[es/postgres] Invalid store stream id: %q / %q", event.StoreStreamID, storeStreamID))
				return
			}
			if event.StreamID != streamID {
				yield(event, fmt.Errorf("[es/postgres] Invalid stream id: %q", event.StreamID))
				return
			}

			if event.EventNumber != eventNumber+1 {
				yield(event, fmt.Errorf("[es/postgres] Invalid eventNumber, last was %d, got %d", eventNumber, event.EventNumber))
				return
			}

			eventNumber = event.EventNumber

			if !yield(event, nil) {
				return
			}

		}
	}
}
