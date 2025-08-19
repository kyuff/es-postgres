package leases

import (
	"testing"

	"github.com/kyuff/es-postgres/internal/assert"
)

func TestDiff(t *testing.T) {
	var tests = []struct {
		name    string
		before  []int
		after   []int
		added   []int
		removed []int
	}{
		{
			name:    "both sides",
			before:  []int{1, 2, 3},
			after:   []int{2, 3, 4},
			added:   []int{4},
			removed: []int{1},
		},
		{
			name:    "just added",
			before:  []int{1, 2, 3},
			after:   []int{1, 2, 3, 4},
			added:   []int{4},
			removed: nil,
		},
		{
			name:    "just removed",
			before:  []int{1, 2, 3},
			after:   []int{1, 2},
			added:   nil,
			removed: []int{3},
		},
		{
			name:    "empty before",
			before:  nil,
			after:   []int{1, 2, 3},
			added:   []int{1, 2, 3},
			removed: nil,
		},
		{
			name:    "empty after",
			before:  []int{1, 2, 3},
			after:   nil,
			added:   nil,
			removed: []int{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// act
			added, removed := diff(tt.before, tt.after)

			// assert
			assert.EqualSlice(t, tt.added, added)
			assert.EqualSlice(t, tt.removed, removed)
		})
	}

}
