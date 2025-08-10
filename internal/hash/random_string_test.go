package hash

import (
	"testing"

	"github.com/kyuff/es-postgres/internal/assert"
)

func TestRandomString(t *testing.T) {
	assert.Equal(t, 16, len(RandomString(16)))
	assert.Equal(t, 0, len(RandomString(0)))
}
