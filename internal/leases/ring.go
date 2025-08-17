package leases

import (
	"slices"
	"sort"
)

type VNode struct {
	VNode    uint32
	NodeName string
	Valid    bool
	Status   Status
}

type Report struct {
	Approve      []VNode
	MissingCount uint32
	UsedVNodes   map[uint32]struct{}
	Values       []uint32
}

type Ring []VNode

func (ring Ring) Analyze(name string, full Range, count uint32) Report {
	sort.Slice(ring, func(i, j int) bool { return ring[i].VNode < ring[j].VNode })

	if len(ring) == 0 {
		return Report{
			MissingCount: count,
			UsedVNodes:   make(map[uint32]struct{}),
		}
	}

	approvals := ring.calculateApprovals(name)
	missing, usedVNodes := ring.calculateMissingAndUsed(name, count)

	return Report{
		Approve:      approvals,
		Values:       ring.calculateValues(name, full, approvals),
		MissingCount: missing,
		UsedVNodes:   usedVNodes,
	}
}

func (ring Ring) calculateApprovals(name string) []VNode {
	var approvals []VNode
	for i, vnode := range ring {
		var next = ring.nextVNode(i)

		if vnode.NodeName == name {
			if vnode.Status == Leased && next.Status == Pending {
				approvals = append(approvals, next)
			}
			if vnode.Status == Pending && next.Status == Pending {
				approvals = append(approvals, next)
			}
		}
	}

	return approvals
}

func (ring Ring) calculateValues(name string, full Range, approvals []VNode) []uint32 {
	var preApproved = make(map[uint32]bool)
	for _, vnode := range approvals {
		preApproved[vnode.VNode] = vnode.NodeName == name
	}

	var values []uint32
	for i, vnode := range ring {
		var next = ring.nextVNode(i)

		if vnode.Status == Leased || preApproved[vnode.VNode] {
			if next.VNode > vnode.VNode {
				values = append(values, fromTo(vnode.VNode, next.VNode)...)
			} else {
				values = append(values, fromTo(full.From, next.VNode)...)
				values = append(values, fromTo(vnode.VNode, full.To)...)
			}
		}
	}

	slices.Sort(values)

	return values
}

func (ring Ring) nextVNode(i int) VNode {
	if len(ring) == 0 {
		return VNode{}
	}
	if len(ring) == 1 {
		return ring[0]
	}

	if len(ring) <= i+1 {
		return ring[0]
	}

	return ring[i+1]
}

func (ring Ring) calculateMissingAndUsed(name string, count uint32) (uint32, map[uint32]struct{}) {
	var (
		seen uint32 = 0
		used        = make(map[uint32]struct{})
	)

	for _, vnode := range ring {
		if vnode.NodeName == name {
			seen++
		}
		used[vnode.VNode] = struct{}{}
	}

	return count - seen, used
}

func fromTo(from, to uint32) []uint32 {
	var items []uint32
	for i := from; i < to; i++ {
		items = append(items, i)
	}
	return items
}
