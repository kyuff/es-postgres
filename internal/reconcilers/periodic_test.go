package reconcilers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/logger"
	"github.com/kyuff/es-postgres/internal/reconcilers"
	"github.com/kyuff/es-postgres/internal/uuid"
)

func TestPeriodic(t *testing.T) {
	const (
		defaultTimeout        = time.Second * 10
		defaultInterval       = time.Millisecond * 10
		defaultProcessTimeout = time.Millisecond * 20
	)
	var (
		newPeriodic = func(c reconcilers.Connector, s reconcilers.Schema, v reconcilers.Valuer) *reconcilers.Periodic {
			return reconcilers.NewPeriodic(logger.Noop{}, c, s, v, defaultInterval, defaultTimeout, defaultProcessTimeout)
		}
		newStream = func() database.Stream {
			return database.Stream{
				StoreID: uuid.V7(),
				Type:    uuid.V7(),
			}
		}
		newStreams = func(n int) []database.Stream {
			var streams []database.Stream
			for range n {
				streams = append(streams, newStream())
			}
			return streams
		}
	)

	t.Run("process all in a single reconcile", func(t *testing.T) {
		// arrange
		var (
			c       = &ConnectorMock{}
			s       = &SchemaMock{}
			v       = &ValuerMock{}
			p       = &ProcessorMock{}
			sut     = newPeriodic(c, s, v)
			streams = newStreams(10)
		)

		c.AcquireReadFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			return &pgxpool.Conn{}, nil
		}
		s.SelectOutboxStreamIDsFunc = func(ctx context.Context, db dbtx.DBTX, graceWindow time.Duration, partitions []uint32, token string, limit int) ([]database.Stream, error) {
			if len(s.SelectOutboxStreamIDsCalls()) > 1 {
				return nil, nil
			}
			return streams, nil
		}
		v.ValuesFunc = func() []uint32 {
			return []uint32{1, 2, 3}
		}
		p.ProcessFunc = func(ctx context.Context, stream database.Stream) error {
			return nil
		}

		// act
		go func() {
			assert.NoError(t, sut.Reconcile(t.Context(), p))
		}()

		// assert
		assert.NoErrorEventually(t, defaultInterval*2, func() error {
			if len(p.ProcessCalls()) < 10 {
				return errors.New("no process calls")
			}

			return nil
		})
		assert.Equal(t, len(streams), len(p.ProcessCalls()))
		assert.Equal(t, 1, len(s.SelectOutboxStreamIDsCalls()))

	})

	t.Run("process repeated with large outbox", func(t *testing.T) {
		// arrange
		var (
			c             = &ConnectorMock{}
			s             = &SchemaMock{}
			v             = &ValuerMock{}
			p             = &ProcessorMock{}
			sut           = newPeriodic(c, s, v)
			expectedToken = uuid.Empty
		)

		c.AcquireReadFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			return &pgxpool.Conn{}, nil
		}
		s.SelectOutboxStreamIDsFunc = func(ctx context.Context, db dbtx.DBTX, graceWindow time.Duration, partitions []uint32, token string, limit int) ([]database.Stream, error) {
			if len(s.SelectOutboxStreamIDsCalls()) > 5 {
				return nil, nil
			}
			assert.Equal(t, expectedToken, token)
			streams := newStreams(limit)
			expectedToken = streams[limit-1].StoreID
			return streams, nil
		}
		v.ValuesFunc = func() []uint32 {
			return []uint32{1, 2, 3}
		}
		p.ProcessFunc = func(ctx context.Context, stream database.Stream) error {
			return nil
		}

		// act
		go func() {
			assert.NoError(t, sut.Reconcile(t.Context(), p))
		}()

		// assert
		assert.NoErrorEventually(t, defaultInterval*2, func() error {
			if len(p.ProcessCalls()) < reconcilers.ReconcileLimit*5 {
				return errors.New("no process calls")
			}

			return nil
		})
		assert.Equal(t, int(reconcilers.ReconcileLimit*5), len(p.ProcessCalls()))
		assert.Equal(t, 5+1, len(s.SelectOutboxStreamIDsCalls()))
		assert.Equal(t, 1, len(c.AcquireReadCalls()))

	})

	t.Run("exit with the context", func(t *testing.T) {
		// arrange
		var (
			ctx, cancel = context.WithCancel(t.Context())
			c           = &ConnectorMock{}
			s           = &SchemaMock{}
			v           = &ValuerMock{}
			p           = &ProcessorMock{}
			sut         = newPeriodic(c, s, v)
		)

		c.AcquireReadFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			return &pgxpool.Conn{}, nil
		}
		s.SelectOutboxStreamIDsFunc = func(ctx context.Context, db dbtx.DBTX, graceWindow time.Duration, partitions []uint32, token string, limit int) ([]database.Stream, error) {
			return newStreams(1), nil
		}
		v.ValuesFunc = func() []uint32 {
			return []uint32{1, 2, 3}
		}
		p.ProcessFunc = func(ctx context.Context, stream database.Stream) error {
			if len(p.ProcessCalls()) == 10 {
				cancel()
			}
			return nil
		}

		// act
		err := sut.Reconcile(ctx, p)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, 10, len(p.ProcessCalls()))
	})

	t.Run("exit with repeated failures from connector", func(t *testing.T) {
		// arrange
		var (
			c   = &ConnectorMock{}
			s   = &SchemaMock{}
			v   = &ValuerMock{}
			p   = &ProcessorMock{}
			sut = newPeriodic(c, s, v)
		)

		c.AcquireReadFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			return nil, errors.New("fail")
		}
		s.SelectOutboxStreamIDsFunc = func(ctx context.Context, db dbtx.DBTX, graceWindow time.Duration, partitions []uint32, token string, limit int) ([]database.Stream, error) {
			return newStreams(1), nil
		}
		v.ValuesFunc = func() []uint32 {
			return []uint32{1, 2, 3}
		}
		p.ProcessFunc = func(ctx context.Context, stream database.Stream) error {
			return nil
		}

		// act
		err := sut.Reconcile(t.Context(), p)

		// assert
		assert.Error(t, err)
		assert.Equal(t, 0, len(p.ProcessCalls()))
	})

	t.Run("exit with repeated failures from schema", func(t *testing.T) {
		// arrange
		var (
			c   = &ConnectorMock{}
			s   = &SchemaMock{}
			v   = &ValuerMock{}
			p   = &ProcessorMock{}
			sut = newPeriodic(c, s, v)
		)

		c.AcquireReadFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			return &pgxpool.Conn{}, nil
		}
		s.SelectOutboxStreamIDsFunc = func(ctx context.Context, db dbtx.DBTX, graceWindow time.Duration, partitions []uint32, token string, limit int) ([]database.Stream, error) {
			return nil, errors.New("fail")
		}
		v.ValuesFunc = func() []uint32 {
			return []uint32{1, 2, 3}
		}
		p.ProcessFunc = func(ctx context.Context, stream database.Stream) error {
			return nil
		}

		// act
		err := sut.Reconcile(t.Context(), p)

		// assert
		assert.Error(t, err)
		assert.Equal(t, 0, len(p.ProcessCalls()))
	})
}
