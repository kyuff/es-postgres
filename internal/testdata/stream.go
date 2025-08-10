package testdata

import "github.com/kyuff/es-postgres/internal/database"

func Stream() database.Stream {
	return database.Stream{
		StoreID: StoreStreamID(),
		Type:    StreamType(),
	}
}
