package reconcilers

import "context"

func FeatureFlag(enabled bool, inner Reconciler) *Flag {
	return &Flag{
		enabled:    enabled,
		reconciler: inner,
	}
}

type Flag struct {
	enabled    bool
	reconciler Reconciler
}

func (f *Flag) Reconcile(ctx context.Context, p Processor) error {
	if f.enabled {
		return f.reconciler.Reconcile(ctx, p)
	}

	<-ctx.Done()
	return nil
}
