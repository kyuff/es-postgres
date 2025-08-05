package database

import (
	"testing"

	"github.com/kyuff/es-postgres/internal/assert"
)

func TestRenderTemplates(t *testing.T) {
	t.Run("render a list of templates", func(t *testing.T) {
		// arrange
		var (
			query1 = "{{ .Prefix }} 1"
			query2 = "{{ .Prefix }} 2"
			query3 = "{{ .Prefix }} 3"
		)

		// act
		err := renderTemplates("Test", &query1, &query2, &query3)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, query1, "Test 1")
		assert.Equal(t, query2, "Test 2")
		assert.Equal(t, query3, "Test 3")
	})

	t.Run("should fail on nil input", func(t *testing.T) {
		// arrange
		var (
			query1         = "{{ .Prefix }} 1"
			query2 *string = nil
		)

		// acdt
		err := renderTemplates("Test", &query1, query2)

		// assert
		assert.Error(t, err)
	})

	t.Run("should fail on empty input", func(t *testing.T) {
		// arrange
		var (
			query1 = "{{ .Prefix }} 1"
			query2 = ""
		)

		// acdt
		err := renderTemplates("Test", &query1, &query2)

		// assert
		assert.Error(t, err)
	})

	t.Run("should fail on mal-formed input", func(t *testing.T) {
		// arrange
		var (
			query1 = "{{ .Prefix }} 1"
			query2 = "{{ ..."
		)

		// acdt
		err := renderTemplates("Test", &query1, &query2)

		// assert
		assert.Error(t, err)
	})
}
