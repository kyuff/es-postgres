package singleflight_test

import (
	"sync"
	"testing"

	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/singleflight"
	"github.com/kyuff/es-postgres/internal/uuid"
)

func TestGroup_TryDo(t *testing.T) {
	t.Run("should execute the function if the key is not locked", func(t *testing.T) {
		// arrange
		var (
			key    = uuid.V7()
			sut    = singleflight.New[string]()
			called = false
		)

		// act
		err := sut.TryDo(key, func() error {
			called = true
			return nil
		})

		// assert
		assert.NoError(t, err)
		assert.Truef(t, called, "called")
	})

	t.Run("should not execute the function if the key is locked", func(t *testing.T) {
		// arrange
		var (
			key      = uuid.V7()
			sut      = singleflight.New[string]()
			called   = false
			prepare  sync.WaitGroup
			complete sync.WaitGroup
		)

		prepare.Add(1)
		go func() {
			_ = sut.TryDo(key, func() error {
				prepare.Done()
				complete.Wait()
				return nil
			})
		}()

		complete.Add(1)
		go func() {
			// arrange
			prepare.Wait()

			// act
			err := sut.TryDo(key, func() error {
				called = true
				return nil
			})

			// assert
			assert.NoError(t, err)
			assert.Truef(t, !called, "called")
			complete.Done()
		}()

		// assert
		complete.Wait()
	})
}
