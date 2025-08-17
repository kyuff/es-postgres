package leases_test

import (
	"math/rand/v2"
	"testing"

	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/leases"
)

func TestRange(t *testing.T) {
	t.Run("return number of items in range", func(t *testing.T) {
		assert.Equal(t, 10, leases.Range{From: 42, To: 52}.Len())
	})

	t.Run("return validity", func(t *testing.T) {
		assert.Truef(t, leases.Range{From: 42, To: 52}.Valid(), "valid")
		assert.Falsef(t, leases.Range{From: 62, To: 52}.Valid(), "invalid")
	})

	t.Run("format string", func(t *testing.T) {
		assert.Equal(t, "42-52", leases.Range{From: 42, To: 52}.String())
	})

	t.Run("values", func(t *testing.T) {
		assert.EqualSlice(t, []uint32{4, 5, 6}, leases.Range{From: 4, To: 7}.Values())
	})

	t.Run("vnode", func(t *testing.T) {
		// arrange
		var (
			ran = rand.New(rand.NewPCG(1, 2))
			sut = leases.Range{From: 4, To: 7}
		)

		// act
		got := sut.VNode(ran)

		// assert
		assert.Equal(t, 6, got)
	})
}
