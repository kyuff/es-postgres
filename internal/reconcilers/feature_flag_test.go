package reconcilers_test

import (
	"context"
	"testing"
	"time"

	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/reconcilers"
)

func TestFeatureFlag(t *testing.T) {
	t.Run("use when enabled", func(t *testing.T) {
		// arrange
		var (
			inner = &ReconcilerMock{}
			p     = &ProcessorMock{}
			flag  = reconcilers.FeatureFlag(true, inner)
		)

		inner.ReconcileFunc = func(ctx context.Context, p reconcilers.Processor) error {
			return nil
		}

		// act
		err := flag.Reconcile(t.Context(), p)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 1, len(inner.ReconcileCalls()))
		assert.Equal(t, reconcilers.Processor(p), inner.ReconcileCalls()[0].P)
	})

	t.Run("no use when disabled", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			inner       = &ReconcilerMock{}
			p           = &ProcessorMock{}
			flag        = reconcilers.FeatureFlag(false, inner)
		)

		inner.ReconcileFunc = func(ctx context.Context, p reconcilers.Processor) error {
			return nil
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		// act
		err := flag.Reconcile(ctx, p)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 0, len(inner.ReconcileCalls()))
	})
}
