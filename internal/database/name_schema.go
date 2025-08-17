package database

import (
	"crypto/sha1"
	"encoding/base32"
	"strings"
	"time"
)

// nameSchema returns a 10-character string composed of:
// - 5-char stable hash (consistent per input)
// - 5-char time-based value (newer => sorts earlier)
func nameSchema(input string) string {
	// Stable hash: SHA-1, base32-encoded
	h := sha1.Sum([]byte(input))
	hash := base32.StdEncoding.EncodeToString(h[:])
	stableHash := hash[:5]

	// Time-based value: ~now inverted, base32-encoded
	now := time.Now().UnixMilli()
	inv := ^now
	timeBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		timeBytes[i] = byte(inv >> (56 - 8*i))
	}

	randomPart := base32.StdEncoding.EncodeToString(timeBytes)
	randomValue := randomPart[8:13]

	return "test_" + strings.ToLower(stableHash+"_"+randomValue)
}
