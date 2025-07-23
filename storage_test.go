package postgres_test

import (
	"testing"

	postgres "github.com/kyuff/es-postgres"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
)

func TestStorage(t *testing.T) {
	t.Skipf("Skipping test %s, requires a running PostgreSQL instance", t.Name())

	t.Run("should create a new storage", func(t *testing.T) {
		// arrange
		var (
			db = database.ConnectTest(t)
		)

		// act
		storage, err := postgres.New(postgres.WithPool(db))

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, storage)
		assert.NoError(t, storage.Close())
	})
}
