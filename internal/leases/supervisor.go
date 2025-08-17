package leases

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/dbtx"
)

type Connector interface {
	AcquireWrite(ctx context.Context) (*pgxpool.Conn, error)
}

type Heartbeater interface {
	Heartbeat(ctx context.Context, conn dbtx.DBTX) ([]uint32, error)
}

type Schema interface {
	RefreshLeases(ctx context.Context, db dbtx.DBTX, nodeName string, ttl time.Duration) (Ring, error)
	ApproveLease(ctx context.Context, db dbtx.DBTX, vnodes []uint32) error
	InsertLease(ctx context.Context, db dbtx.DBTX, vnode uint32, name string, ttl time.Duration, status string) error
}

type Supervisor struct {
	cfg       *Config
	heartbeat Heartbeater
	connector Connector

	mu     sync.RWMutex
	values []uint32
}

func NewSupervisor(connector Connector, schema Schema, opts ...Option) (*Supervisor, error) {
	cfg := applyOptions(DefaultOptions(), opts...)
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Supervisor{
		cfg:       cfg,
		heartbeat: NewHeartbeat(cfg, schema),
		connector: connector,
	}, nil
}

func (s *Supervisor) Values() []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.values
}

// Start is blocking and keeps the Values up to date.
func (s *Supervisor) Start(ctx context.Context) error {
	var ticker = time.NewTicker(s.cfg.HeartbeatInterval)

	err := s.tick(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.tick(ctx); err != nil {
				return err
			}
		}
	}
}

func (s *Supervisor) tick(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.HeartbeatTimeout)
	defer cancel()

	conn, err := s.connector.AcquireWrite(ctx)
	if err != nil {
		return err
	}

	defer conn.Release()

	values, err := s.heartbeat.Heartbeat(ctx, conn)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.values = values

	return nil
}
