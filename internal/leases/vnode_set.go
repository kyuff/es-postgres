package leases

type vnodeSet map[uint32]bool

func (set vnodeSet) has(v uint32) bool {
	_, ok := set[v]
	return ok
}

func (set vnodeSet) add(v uint32) {
	set[v] = true
}
