package leases_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/leases"
)

func TestSupervisor(t *testing.T) {
	t.Run("fail with illegal option", func(t *testing.T) {
		// arrange
		var (
			connector = &ConnectorMock{}
			schema    = &SchemaMock{}
		)

		// act
		_, err := leases.NewSupervisor(connector, schema, leases.WithRange(leases.Range{From: 220, To: 100}))

		// assert
		assert.Error(t, err)
	})

	t.Run("return valid supervisor", func(t *testing.T) {
		// arrange
		var (
			connector = &ConnectorMock{}
			schema    = &SchemaMock{}
		)

		// act
		supervisor, err := leases.NewSupervisor(connector, schema, leases.WithRange(leases.Range{From: 0, To: 100}))

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, supervisor)
	})

	t.Run("update values from heartbeat", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			connector   = &ConnectorMock{}
			heartbeater = &HeartbeaterMock{}
			cfg         = leases.DefaultOptions()
			sut         = leases.New(cfg, heartbeater, connector)

			wg       = &sync.WaitGroup{}
			expected = []uint32{1, 2, 3}
		)

		leases.WithHeartbeatInterval(time.Millisecond * 50)(cfg)

		connector.AcquireWriteFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			return &pgxpool.Conn{}, nil
		}
		heartbeater.HeartbeatFunc = func(ctx context.Context, conn dbtx.DBTX) ([]uint32, error) {
			defer cancel()
			return expected, nil
		}

		wg.Add(1)

		go func() {
			err := sut.Start(ctx)
			assert.NoError(t, err)
			wg.Done()
		}()

		wg.Wait()

		// act
		got := sut.Values()

		// assert
		assert.EqualSlice(t, expected, got)
	})

	t.Run("work even if connector fails first", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			connector   = &ConnectorMock{}
			heartbeater = &HeartbeaterMock{}
			cfg         = leases.DefaultOptions()
			sut         = leases.New(cfg, heartbeater, connector)

			wg       = &sync.WaitGroup{}
			expected = []uint32{1, 2, 3}
		)

		leases.WithHeartbeatInterval(time.Millisecond * 50)(cfg)

		connector.AcquireWriteFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			if len(connector.AcquireWriteCalls()) == 1 {
				return nil, errors.New("fail")
			}
			return &pgxpool.Conn{}, nil
		}
		heartbeater.HeartbeatFunc = func(ctx context.Context, conn dbtx.DBTX) ([]uint32, error) {
			defer cancel()
			return expected, nil
		}

		wg.Add(1)

		go func() {
			err := sut.Start(ctx)
			assert.NoError(t, err)
			wg.Done()
		}()

		wg.Wait()

		// act
		got := sut.Values()

		// assert
		assert.EqualSlice(t, expected, got)
	})

	t.Run("work even if heartbeat fails first", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			connector   = &ConnectorMock{}
			heartbeater = &HeartbeaterMock{}
			cfg         = leases.DefaultOptions()
			sut         = leases.New(cfg, heartbeater, connector)

			wg       = &sync.WaitGroup{}
			expected = []uint32{1, 2, 3}
		)

		leases.WithHeartbeatInterval(time.Millisecond * 50)(cfg)

		connector.AcquireWriteFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			return &pgxpool.Conn{}, nil
		}
		heartbeater.HeartbeatFunc = func(ctx context.Context, conn dbtx.DBTX) ([]uint32, error) {
			if len(heartbeater.HeartbeatCalls()) == 1 {
				return nil, errors.New("fail")
			}

			defer cancel()
			return expected, nil
		}

		wg.Add(1)

		go func() {
			err := sut.Start(ctx)
			assert.NoError(t, err)
			wg.Done()
		}()

		wg.Wait()

		// act
		got := sut.Values()

		// assert
		assert.EqualSlice(t, expected, got)
	})

	t.Run("exit with error if heartbeat keeps failing", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			connector   = &ConnectorMock{}
			heartbeater = &HeartbeaterMock{}
			cfg         = leases.DefaultOptions()
			sut         = leases.New(cfg, heartbeater, connector)

			wg = &sync.WaitGroup{}
		)

		leases.WithHeartbeatInterval(time.Millisecond * 50)(cfg)

		connector.AcquireWriteFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			return &pgxpool.Conn{}, nil
		}
		heartbeater.HeartbeatFunc = func(ctx context.Context, conn dbtx.DBTX) ([]uint32, error) {
			return nil, errors.New("fail")
		}

		wg.Add(1)

		go func() {
			// act
			err := sut.Start(ctx)

			// assert
			assert.Error(t, err)
			wg.Done()
			cancel()
		}()

		wg.Wait()
	})
}
