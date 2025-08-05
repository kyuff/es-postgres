package postgres_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	postgres "github.com/kyuff/es-postgres"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/seqs"
	"github.com/kyuff/es-postgres/internal/uuid"
)

type EventA struct {
}

func (e EventA) EventName() string {
	return "EventA"
}

type EventB struct {
}

func (e EventB) EventName() string {
	return "EventB"
}

func TestStorage(t *testing.T) {
	var (
		newStreamType = uuid.V7
		newStreamID   = uuid.V7
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
}
