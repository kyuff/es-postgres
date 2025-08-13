package leases

import (
	"context"
	"sync"
	"time"

	"github.com/kyuff/es-postgres/internal/dbtx"
)

type Schema interface {
	SelectLeases(ctx context.Context, db dbtx.DBTX) (Ring, error)
	ApproveLease(ctx context.Context, db dbtx.DBTX, vnodes []uint32) error
	InsertLease(ctx context.Context, db dbtx.DBTX, vnode uint32, name string, ttl time.Duration, status string) error
}

type Supervisor struct {
	cfg    *Config
	schema Schema

	mu     sync.RWMutex
	values []uint32
}

func NewSupervisor(schema Schema, opts ...Option) (*Supervisor, error) {
	cfg := applyOptions(DefaultOptions(), opts...)
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Supervisor{
		cfg:    cfg,
		values: nil,
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// TODO
		}
	}
}
