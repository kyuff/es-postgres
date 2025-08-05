package postgres_test

import (
	"testing"

	postgres "github.com/kyuff/es-postgres"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
)

func TestStorage(t *testing.T) {
	t.Run("should create a new storage from dsn", func(t *testing.T) {
		// arrange
		var (
			connector = postgres.InstanceFromDSN(database.DSNTest(t))
		)

		// act
		storage, err := postgres.New(connector, postgres.WithDefaultSlog())

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, storage)
		assert.NoError(t, storage.Close())
	})

	t.Run("should create a new storage from pool", func(t *testing.T) {
		// arrange
		var (
			dsn       = database.DSNTest(t)
			pool, err = database.Connect(t.Context(), dsn)
			_         = assert.NoError(t, err)
			connector = postgres.InstanceFromPool(pool)
		)

		// act
		storage, err := postgres.New(connector)

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, storage)
		assert.NoError(t, storage.Close())
	})
}
