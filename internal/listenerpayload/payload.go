package listenerpayload

import (
	"fmt"
	"strings"

	"github.com/kyuff/es"
)

func Encode(ref es.StreamReference) string {
	return ref.StreamType + ":" + ref.StoreStreamID + ":" + ref.StreamID
}

func Decode(payload string) (es.StreamReference, error) {
	parts := strings.Split(payload, ":")
	if len(parts) != 3 {
		return es.StreamReference{}, fmt.Errorf("malformed payload: %s", payload)
	}

	return es.StreamReference{
		StreamType:    parts[0],
		StoreStreamID: parts[1],
		StreamID:      parts[2],
	}, nil
}
