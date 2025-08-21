package reconcilers

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/dbtx"
)

type Valuer interface {
	Values() []uint32
}

type Connector interface {
	AcquireRead(ctx context.Context) (*pgxpool.Conn, error)
}

type Schema interface {
	SelectOutboxStreamIDs(ctx context.Context, db dbtx.DBTX, graceWindow time.Duration, partitions []uint32, token string, limit int) ([]es.StreamReference, error)
}

type Logger interface {
	InfofCtx(ctx context.Context, template string, args ...any)
	ErrorfCtx(ctx context.Context, template string, args ...any)
}

type Processor interface {
	Process(ctx context.Context, stream es.StreamReference) error
}
