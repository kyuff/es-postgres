package eventsio_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/eventsio"
	"github.com/kyuff/es-postgres/internal/seqs"
	"github.com/kyuff/es-postgres/internal/testdata"
)

func TestEventWriter(t *testing.T) {
	var (
		newPartitioner = func(partition uint32) func(streamType, streamID string) uint32 {
			return func(streamType, streamID string) uint32 {
				return partition
			}
		}
	)

	t.Run("do nothing on no events", func(t *testing.T) {
		// arrange
		var (
			db          = &pgxpool.Pool{}
			schema      = &SchemaMock{}
			partitioner = newPartitioner(1)
			w           = eventsio.NewWriter(schema, eventsio.NewValidator(), partitioner)
			streamType  = testdata.StreamType()
			events      = testdata.Events(0, func(e *es.Event) {
				e.StreamType = streamType
			})
		)

		// act
		err := w.Write(t.Context(), db, streamType, seqs.Seq2(events...))

		// assert
		assert.NoError(t, err)
	})
}
