package leases

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/retry"
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

type Leases struct {
	cfg       *Config
	heartbeat Heartbeater
	connector Connector

	mu     sync.RWMutex
	values []uint32
}

func New(connector Connector, schema Schema, opts ...Option) (*Leases, error) {
	cfg := applyOptions(DefaultOptions(), opts...)
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return NewLeases(cfg, NewHeartbeat(cfg, schema), connector), nil
}

func NewLeases(cfg *Config, heartbeat Heartbeater, connector Connector) *Leases {
	return &Leases{
		cfg:       cfg,
		heartbeat: heartbeat,
		connector: connector,
	}
}

func (s *Leases) Values() []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.values
}

// Start is blocking and keeps the Values up to date.
func (s *Leases) Start(ctx context.Context) error {
	err := retry.Continue(ctx, s.cfg.HeartbeatInterval, 10, s.tick)
	if err != nil {
		return fmt.Errorf("supervisor failed: %w", err)
	}

	return nil
}

func (s *Leases) tick(ctx context.Context) error {
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
