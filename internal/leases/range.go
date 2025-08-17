package leases

import (
	"fmt"
	"math/rand/v2"
)

type Range struct {
	From uint32
	To   uint32
}

func (r Range) Len() uint32 {
	return r.To - r.From
}

func (r Range) Valid() bool {
	return r.From < r.To
}

func (r Range) String() string {
	return fmt.Sprintf("%d-%d", r.From, r.To)
}

func (r Range) VNode(ran *rand.Rand) uint32 {
	return ran.Uint32N(r.To-r.From) + r.From
}

func (r Range) Values() []uint32 {
	values := make([]uint32, r.To-r.From)
	for i := range r.To - r.From {
		values[i] = r.From + uint32(i)
	}
	return values
}
