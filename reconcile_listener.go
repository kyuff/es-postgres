package postgres

import (
	"context"
)

func newReconcileListener() *reconcileListener {
	return &reconcileListener{}
}

type reconcileListener struct {
}

func (l *reconcileListener) Reconcile(ctx context.Context, p processor) error {
	return nil
}
