package publish

import (
	"context"

	"github.com/kyuff/es"
)

func NewDatabaseListener() *DatabaseListener {
	return &DatabaseListener{}
}

type DatabaseListener struct {
}

func (l *DatabaseListener) Publish(ctx context.Context, w es.Writer) error {
	return nil
}
