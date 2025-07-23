package migrate

import "hash/fnv"

func calculateProcessID(prefix string) int {
	h := fnv.New32()
	_, _ = h.Write([]byte(prefix))
	return int(h.Sum32())
}
