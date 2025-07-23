package postgres

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
)

func New(opts ...Option) (*Storage, error) {
	cfg := applyOptions(defaultOptions(), opts...)
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("[es/postgres] invalid configuration: %w", err)
	}

	ctx := cfg.startCtx()

	pool, err := cfg.poolNew(ctx)
	if err != nil {
		return nil, fmt.Errorf("[es/postgres] failed connecting to database: %w", err)
	}

	return &Storage{
		cfg:  cfg,
		pool: pool,
	}, nil
}

type Storage struct {
	conn *pgxpool.Pool
	cfg  *Config
	pool *pgxpool.Pool
}

func (s *Storage) Read(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[es.Event, error] {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) Write(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) StartPublish(ctx context.Context, w es.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) Register(streamType string, types ...es.Content) error {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) GetStreamIDs(ctx context.Context, streamType string, storeStreamID string, limit int64) ([]string, string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) Close() error {
	return errors.Join(
		s.cfg.poolClose(),
	)
}
