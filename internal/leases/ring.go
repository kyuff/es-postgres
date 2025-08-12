package leases

type Info struct {
	VNode    uint32
	NodeName string
	Valid    bool
	Status   Status
}

type Report struct {
	Approve       []Info
	Ranges        []Range
	MissingCount  uint32
	BlockedVNodes map[uint32]struct{}
}

type Ring []Info

func (ring Ring) Analyze(name string, full Range, count uint32) Report {
	var (
		seen uint32 = 0
	)

	for _, info := range ring {
		if name == info.NodeName {
			seen++
		}
	}

	return Report{
		Approve:       []Info{},
		Ranges:        []Range{},
		MissingCount:  count - seen,
		BlockedVNodes: make(map[uint32]struct{}),
	}
}
