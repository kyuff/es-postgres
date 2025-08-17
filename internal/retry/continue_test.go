package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/retry"
)

func TestContinue(t *testing.T) {
	t.Run("fail after max errors", func(t *testing.T) {
		// arrange
		var (
			calls = 0
		)

		// act
		err := retry.Continue(t.Context(), time.Millisecond, 5, func(ctx context.Context) error {
			calls++
			return errors.New("test")
		})

		// assert
		assert.Error(t, err)
		assert.Equal(t, 5, calls)
	})

	t.Run("exit with context", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			calls       = 0
		)

		// act
		err := retry.Continue(ctx, time.Millisecond, 5, func(ctx context.Context) error {
			calls++
			if calls == 5 {
				cancel()
			}
			return nil
		})

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 5, calls)
	})
}
