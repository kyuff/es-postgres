package postgres

import (
	"context"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/database"
)

type processor interface {
	Process(ctx context.Context, stream database.Stream) error
}

func newProcessWriter(w es.Writer) *processWriter {
	return &processWriter{
		w: w,
	}
}

type processWriter struct {
	w es.Writer
}

func (p *processWriter) Process(ctx context.Context, stream database.Stream) error {
	return nil
}
