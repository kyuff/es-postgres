package leases

import (
	"math"
	"testing"

	"github.com/kyuff/es-postgres/internal/assert"
)

func TestNewVNode(t *testing.T) {

	var asserCorrectVNode = func(t *testing.T, from, to uint32) {
		// act
		got := newVNode(from, to)

		// assert
		assert.Truef(t, got >= from, "%d >= %d", got, from)
		assert.Truef(t, got < to, "%d < %d", got, from)
	}

	asserCorrectVNode(t, 0, 1)
	asserCorrectVNode(t, 0, 1024)
	asserCorrectVNode(t, 10, 20)
	asserCorrectVNode(t, 0, math.MaxUint32)
}
