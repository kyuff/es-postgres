package hash

import "hash/fnv"

func FNV(s string, max uint32) uint32 {
	hash := fnv.New32()
	_, _ = hash.Write([]byte(s))
	return hash.Sum32() % max
}
