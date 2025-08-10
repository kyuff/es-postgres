package leases

import (
	"math/rand/v2"
	"time"
)

var r *rand.Rand

func init() {
	r = rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
}

// newVNode generates a random vnode number between from and to
// Wrong input where from > to will cause overflow.
func newVNode(from, to uint32) uint32 {
	return r.Uint32N(to-from) + from
}
