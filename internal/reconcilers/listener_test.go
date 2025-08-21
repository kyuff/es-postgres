package reconcilers_test

import (
	"context"
	"testing"

	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/reconcilers"
)

func TestNewListener(t *testing.T) {
	t.Run("should listen to the outbox", func(t *testing.T) {
		// arrange
		var (
			listener = reconcilers.NewListener()

			p = &ProcessorMock{}
		)

		p.ProcessFunc = func(ctx context.Context, stream es.StreamReference) error {
			return nil
		}

		// act
		err := listener.Reconcile(t.Context(), p)

		// assert
		assert.NoError(t, err)
	})
}
