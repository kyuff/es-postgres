package processor

import (
	"context"
	"iter"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
)

type Connector interface {
	AcquireWriteStream(ctx context.Context, streamType, streamID string) (*pgxpool.Conn, error)
}

type Reader interface {
	Read(ctx context.Context, streamType, streamID string, eventNumber int64) iter.Seq2[es.Event, error]
}
