package database

import (
	"testing"
	"time"

	"github.com/kyuff/es-postgres/internal/assert"
)

func Test_schemaName(t *testing.T) {
	// arrange
	var (
		schemas []string
	)

	// act
	for range 5 {
		schemas = append(schemas, schemaName(t.Name()))
		time.Sleep(time.Millisecond * 100)
	}

	// assert
	if assert.Equal(t, 5, len(schemas)) {
		set := make(map[string]struct{})
		prefix := schemas[0][0:5]
		for _, schema := range schemas {
			assert.Equal(t, prefix, schema[0:5])
			set[schema] = struct{}{}
		}

		assert.Equal(t, 5, len(set))
	}
}
