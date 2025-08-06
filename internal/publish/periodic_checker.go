package publish

import (
	"context"

	"github.com/kyuff/es"
)

func NewPeriodicChecker() *PeriodicChecker {
	return &PeriodicChecker{}
}

type PeriodicChecker struct {
}

func (h *PeriodicChecker) Publish(ctx context.Context, w es.Writer) error {
	return nil
}
