package leases

import "cmp"

// diff returns what is added or removed between before and after.
// It is expected inputs to be sorted.
func diff[T cmp.Ordered](before, after []T) ([]T, []T) {
	var added, removed []T
	i, j := 0, 0

	for i < len(before) && j < len(after) {
		if before[i] == after[j] {
			// same element, skip both
			i++
			j++
		} else if before[i] < after[j] {
			// before[i] is missing in after → removed
			removed = append(removed, before[i])
			i++
		} else {
			// after[j] is new → added
			added = append(added, after[j])
			j++
		}
	}

	// leftovers
	for ; i < len(before); i++ {
		removed = append(removed, before[i])
	}
	for ; j < len(after); j++ {
		added = append(added, after[j])
	}

	return added, removed
}
