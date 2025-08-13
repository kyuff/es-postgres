package leases

import (
	"slices"
	"sort"
)

type Info struct {
	VNode    uint32
	NodeName string
	Valid    bool
	Status   Status
}

type Report struct {
	Approve       []Info
	MissingCount  uint32
	BlockedVNodes map[uint32]struct{}
	Values        []uint32
}

type Ring []Info

func (ring Ring) Analyze(name string, full Range, count uint32) Report {
	sort.Slice(ring, func(i, j int) bool { return ring[i].VNode < ring[j].VNode })

	if len(ring) == 0 {
		return Report{
			MissingCount:  count,
			BlockedVNodes: make(map[uint32]struct{}),
		}
	}

	if len(ring) == 1 {
		var (
			missing = count
			values  []uint32
		)

		if ring[0].NodeName == name {
			missing--
			values = full.Values()
		}

		return Report{
			Approve:      nil,
			Values:       values,
			MissingCount: missing,
			BlockedVNodes: map[uint32]struct{}{
				ring[0].VNode: {},
			},
		}
	}

	var (
		seen      uint32 = 0
		approve   []Info
		values    []uint32
		blocked   = make(map[uint32]struct{})
		lastOwned *Info
	)

	for _, info := range ring {
		blocked[info.VNode] = struct{}{}
		if name == info.NodeName {
			seen++
			if lastOwned == nil {
				lastOwned = &info
				continue
			}

			values = append(values, Range{
				From: lastOwned.VNode,
				To:   info.VNode,
			}.Values()...)

			lastOwned = &info
		} else {
			if info.Status == Pending {
				approve = append(approve, info)
			}

			if lastOwned != nil {
				values = append(values, Range{
					From: lastOwned.VNode,
					To:   info.VNode,
				}.Values()...)
				lastOwned = nil
			}

		}

	}

	if lastOwned != nil {
		// head
		values = append(values, Range{
			From: full.From,
			To:   ring[0].VNode,
		}.Values()...)
		// tail
		values = append(values, Range{
			From: lastOwned.VNode,
			To:   full.To,
		}.Values()...)
	}

	slices.Sort(values)

	return Report{
		Approve:       approve,
		Values:        values,
		MissingCount:  count - seen,
		BlockedVNodes: blocked,
	}
}
