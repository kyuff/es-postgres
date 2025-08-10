package hash

import (
	"math/rand/v2"
	"time"
)

func RandomString(size uint8) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))

	b := make([]byte, size)
	for i := range b {
		b[i] = letters[r.IntN(len(letters))]
	}

	return string(b)
}
