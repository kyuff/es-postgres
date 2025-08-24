package reconcilers_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/listenerpayload"
	"github.com/kyuff/es-postgres/internal/logger"
	"github.com/kyuff/es-postgres/internal/reconcilers"
	"github.com/kyuff/es-postgres/internal/testdata"
)

func TestNewListener(t *testing.T) {
	type fixture struct {
		schema        *database.Schema
		pool          *pgxpool.Pool
		schemaMock    *SchemaMock
		logger        reconcilers.Logger
		connectorMock *ConnectorMock
		processor     *ProcessorMock
		ctx           context.Context
		cancel        context.CancelFunc
	}
	var (
		values = func(from, to uint32) []uint32 {
			var v []uint32
			for i := from; i < to; i++ {
				v = append(v, i)
			}
			return v
		}
		newSchema = func(t *testing.T, ctx context.Context) (*pgxpool.Pool, *database.Schema) {
			t.Helper()
			dsn := database.DSNTest(t)
			pool, err := database.Connect(ctx, dsn)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			t.Cleanup(pool.Close)
			schema, err := database.NewSchema("es")
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			err = database.Migrate(t.Context(), pool, schema)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			t.Cleanup(func() {
				pool.Close()
			})
			return pool, schema
		}
		newFixture = func(t *testing.T, mods ...func(*fixture)) *fixture {
			var (
				ctx, cancel  = context.WithCancel(t.Context())
				pool, schema = newSchema(t, ctx)
				connector    = &ConnectorMock{}
				schemaMock   = &SchemaMock{}
				processor    = &ProcessorMock{}
			)

			schemaMock.ListenFunc = func(ctx context.Context, db dbtx.DBTX, partitions []uint32) error {
				return schema.Listen(ctx, db, partitions)
			}
			schemaMock.UnlistenFunc = func(ctx context.Context, db dbtx.DBTX, partitions []uint32) error {
				return schema.Unlisten(ctx, db, partitions)
			}
			connector.AcquireWriteFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
				return pool.Acquire(ctx)
			}
			processor.ProcessFunc = func(ctx context.Context, stream es.StreamReference) error {
				return nil
			}

			t.Cleanup(cancel)

			f := &fixture{
				schema:        schema,
				pool:          pool,
				schemaMock:    schemaMock,
				logger:        logger.Noop{},
				connectorMock: connector,
				processor:     processor,
				ctx:           ctx,
				cancel:        cancel,
			}
			for _, mod := range mods {
				mod(f)
			}

			return f
		}
	)
	t.Run("should listen to value changed", func(t *testing.T) {
		// arrange
		var (
			f        = newFixture(t)
			timeout  = time.Millisecond * 100
			listener = reconcilers.NewListener(f.connectorMock, f.schemaMock, f.logger, timeout)
		)

		f.schemaMock.ListenFunc = func(ctx context.Context, db dbtx.DBTX, partitions []uint32) error {
			defer f.cancel()
			return f.schema.Listen(ctx, db, partitions)
		}

		go func() {
			err := listener.ValuesChanged(values(0, 10), values(0, 10), nil)
			assert.NoError(t, err)
		}()

		// act
		err := listener.Reconcile(f.ctx, f.processor)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 1, len(f.schemaMock.ListenCalls()))
	})

	t.Run("should process streams", func(t *testing.T) {
		// arrange
		var (
			f        = newFixture(t)
			timeout  = time.Millisecond * 100
			listener = reconcilers.NewListener(f.connectorMock, f.schemaMock, f.logger, timeout)

			stream = testdata.StreamReference()
		)

		f.processor.ProcessFunc = func(ctx context.Context, got es.StreamReference) error {
			defer f.cancel()

			assert.Equal(t, stream, got)
			return nil
		}

		go func() {
			assert.NoError(t, listener.ValuesChanged(values(0, 10), values(0, 10), nil))
			assert.NoError(t, f.schema.Notify(t.Context(), f.pool, 2, listenerpayload.Encode(stream)))
		}()

		// act
		err := listener.Reconcile(f.ctx, f.processor)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 1, len(f.processor.ProcessCalls()))
	})

	t.Run("should not process streams with foreign value", func(t *testing.T) {
		// arrange
		var (
			f        = newFixture(t)
			timeout  = time.Millisecond * 100
			listener = reconcilers.NewListener(f.connectorMock, f.schemaMock, f.logger, timeout)

			stream = testdata.StreamReference()
			other  = testdata.StreamReference()
		)

		f.processor.ProcessFunc = func(ctx context.Context, got es.StreamReference) error {
			defer f.cancel()

			assert.Equal(t, stream, got)
			return nil
		}

		go func() {
			assert.NoError(t, listener.ValuesChanged(values(0, 10), values(0, 10), nil))
			assert.NoError(t, f.schema.Notify(t.Context(), f.pool, 300, listenerpayload.Encode(other)))
			assert.NoError(t, f.schema.Notify(t.Context(), f.pool, 2, listenerpayload.Encode(stream)))
		}()

		// act
		err := listener.Reconcile(f.ctx, f.processor)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 1, len(f.processor.ProcessCalls()))
	})
}
