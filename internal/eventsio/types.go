package eventsio

import (
	"context"
	"iter"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/dbtx"
)

type Schema interface {
	WriteEvent(ctx context.Context, db dbtx.DBTX, event es.Event, content, metadata []byte) error
	InsertOutbox(ctx context.Context, tx dbtx.DBTX, streamType, streamID, storeStreamID string, eventNumber, watermark int64, partition uint32) (int64, error)
	UpdateOutbox(ctx context.Context, tx dbtx.DBTX, streamType, streamID string, eventNumber, lastEventNumber int64) (int64, error)
	SelectEvents(ctx context.Context, db dbtx.DBTX, streamType string, streamID string, eventNumber int64) (pgx.Rows, error)
}

type Validator interface {
	Validate(streamType string, events iter.Seq2[es.Event, error]) iter.Seq2[es.Event, error]
}

type Connector interface {
	AcquireReadStream(ctx context.Context, streamType, streamID string) (*pgxpool.Conn, error)
}

type Codec interface {
	Encode(event es.Event) ([]byte, error)
	Decode(streamType, contentName string, b []byte) (es.Content, error)
	Register(streamType string, contentTypes ...es.Content) error
}

type ValidatorFunc func(streamType string, events iter.Seq2[es.Event, error]) iter.Seq2[es.Event, error]

func (fn ValidatorFunc) Validate(streamType string, events iter.Seq2[es.Event, error]) iter.Seq2[es.Event, error] {
	return fn(streamType, events)
}
