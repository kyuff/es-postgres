package postgres

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/database"
)

type Connector interface {
	// Ping will always be the first call a Storage does to a Connector.
	Ping(ctx context.Context) error

	// ApplyMigrations must call apply with a Conn for all instances that must have DDL done.
	ApplyMigrations(ctx context.Context, apply func(conn *pgxpool.Conn) error) error

	// Close must free all underlying resources
	Close() error

	// AcquireRead supplies a connection used to read
	AcquireRead(ctx context.Context) (*pgxpool.Conn, error)

	// AcquireReadStream supplies a connection used to read from the given stream
	AcquireReadStream(ctx context.Context, streamType, streamID string) (*pgxpool.Conn, error)

	// AcquireWrite supplies a connection used to write
	AcquireWrite(ctx context.Context) (*pgxpool.Conn, error)

	// AcquireWriteStream supplies a connection used to write to the given stream
	AcquireWriteStream(ctx context.Context, streamType, streamID string) (*pgxpool.Conn, error)

	// isConnector is a marker to enforce package implementations for now.
	isConnector()
}

func InstanceFromDSN(dsn string) *Instance {
	return &Instance{
		dsn: dsn,
	}
}

func InstanceFromPool(pool *pgxpool.Pool) *Instance {
	return &Instance{
		pool: pool,
	}
}

type Instance struct {
	dsn string

	mu   sync.RWMutex
	pool *pgxpool.Pool
}

func (i *Instance) isConnector() {}

func (i *Instance) Ping(ctx context.Context) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.pool == nil {
		var err error
		i.pool, err = database.Connect(ctx, i.dsn)
		if err != nil {
			return err
		}
	}

	return i.pool.Ping(ctx)
}

func (i *Instance) ApplyMigrations(ctx context.Context, apply func(conn *pgxpool.Conn) error) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.pool.AcquireFunc(ctx, apply)
}

func (i *Instance) AcquireWrite(ctx context.Context) (*pgxpool.Conn, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.pool.Acquire(ctx)
}

func (i *Instance) AcquireWriteStream(ctx context.Context, streamType, streamID string) (*pgxpool.Conn, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.pool.Acquire(ctx)
}

func (i *Instance) AcquireRead(ctx context.Context) (*pgxpool.Conn, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.pool.Acquire(ctx)
}

func (i *Instance) AcquireReadStream(ctx context.Context, streamType, streamID string) (*pgxpool.Conn, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.pool.Acquire(ctx)
}

func (i *Instance) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.pool != nil {
		i.pool.Close()
	}

	return nil
}
