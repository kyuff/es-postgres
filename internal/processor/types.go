package processor

import (
	"context"
	"iter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/dbtx"
)

type Connector interface {
	AcquireWriteStream(ctx context.Context, streamType, streamID string) (*pgxpool.Conn, error)
}

type Reader interface {
	Read(ctx context.Context, streamType, streamID string, eventNumber int64) iter.Seq2[es.Event, error]
}

type Schema interface {
	SelectOutboxWatermark(ctx context.Context, db dbtx.DBTX, stream database.Stream) (database.OutboxWatermark, int64, error)
	UpdateOutboxWatermark(ctx context.Context, db dbtx.DBTX, stream database.Stream, delay time.Duration, watermark database.OutboxWatermark) error
}
