package leases

import (
	"testing"

	"github.com/kyuff/es-postgres/internal/assert"
)

func TestVNodeSet(t *testing.T) {
	// arrange
	var (
		sut = make(vnodeSet)
	)

	// act
	sut.add(5)

	// assert
	assert.Truef(t, sut.has(5), "should be there")
	assert.Falsef(t, sut.has(2), "should not be there")
}
