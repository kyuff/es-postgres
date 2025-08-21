package testdata

import (
	"time"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/uuid"
)

func StreamReferences(streamType string, count int, mods ...func(ref *es.StreamReference)) []es.StreamReference {
	var refs []es.StreamReference
	var ids = uuid.V7At(time.Now(), count)
	var storeIDs = uuid.V7At(time.Now(), count)
	for i := range count {
		ref := es.StreamReference{
			StreamType:    streamType,
			StreamID:      ids[i],
			StoreStreamID: storeIDs[i],
		}
		for _, mod := range mods {
			mod(&ref)
		}
		refs = append(refs, ref)
	}
	return refs
}

func StreamReference(mods ...func(ref *es.StreamReference)) es.StreamReference {
	ref := es.StreamReference{
		StreamType:    StreamType(),
		StreamID:      StreamID(),
		StoreStreamID: StoreStreamID(),
	}
	for _, mod := range mods {
		mod(&ref)
	}
	return ref
}
