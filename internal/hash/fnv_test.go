package hash_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/hash"
)

func TestFNV(t *testing.T) {
	t.Run("should be consistent", func(t *testing.T) {
		assert.Equal(t, 42, hash.FNV("hash 42/128 (am)", 128))
		assert.Equal(t, 42, hash.FNV("hash 42/128 (fx)", 128))
		assert.Equal(t, 42, hash.FNV("hash 42/128 (sc)", 128))
		assert.Equal(t, 42, hash.FNV("hash 42/128 (xf)", 128))
		assert.Equal(t, 42, hash.FNV("hash 42/128 (yu)", 128))
	})

	t.Run("should distribute equally", func(t *testing.T) {
		// arrange
		var (
			iterations           = 500_000
			mod           uint32 = 128
			expectedCount        = float64(iterations) / float64(mod)
			diff          float64

			results = map[uint32]uint64{}
			randStr = func() string {
				const charset = "abcdefghijklmnopqrstuvwxyz0123456789-"
				result := make([]byte, 10)

				for i := range result {
					result[i] = charset[rand.IntN(len(charset))]
				}

				return string(result)
			}
		)

		// act
		for range iterations {
			results[hash.FNV(randStr(), mod)]++
		}

		// assert
		for key, count := range results {
			diff = math.Abs((float64(count) - expectedCount) / expectedCount)
			assert.Truef(t, diff < 0.10, "diff above thresshold of 10%%")
			assert.Truef(t, key < mod, "max value")
		}

		assert.Equal(t, int(mod), len(results))
	})
}

func findSolution(target, max uint32) []string {
	const (
		template = "hash %d/%d (%s)"
		seed     = "abcdefghijklmnopqrstuvwxyz"
	)

	var matches []string
	for i := 0; i < len(seed); i++ {
		for j := 0; j < len(seed); j++ {
			attempt := fmt.Sprintf(template, target, max, string(seed[i])+string(seed[j]))
			if hash.FNV(attempt, max) == target {
				matches = append(matches, attempt)
			}
		}
	}

	return matches
}
