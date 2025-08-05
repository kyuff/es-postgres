package postgres

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/database"
)

func New(connector Connector, opts ...Option) (*Storage, error) {
	cfg := applyOptions(defaultOptions(), opts...)
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("[es/postgres] invalid configuration: %w", err)
	}

	ctx := cfg.startCtx()

	err := connector.Ping(ctx)
	if err != nil {
		return nil, err
	}

	schema, err := database.NewSchema(cfg.tablePrefix)
	if err != nil {
		return nil, err
	}

	err = connector.ApplyMigrations(ctx, func(conn *pgxpool.Conn) error {
		err := database.Migrate(ctx, conn, schema)
		if err != nil {
			cfg.logger.ErrorfCtx(ctx, "[es/postgres] Database migration failed for %q: %s", database.ToDSN(conn), err)
			return err
		}

		cfg.logger.InfofCtx(ctx, "[es/postgres] Database migrated %q", database.ToDSN(conn))
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Storage{
		cfg:       cfg,
		connector: connector,
		schema:    schema,
	}, nil
}

type Storage struct {
	cfg       *Config
	connector Connector
	schema    *database.Schema
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
	return s.cfg.codec.Register(streamType, types...)
}

func (s *Storage) GetStreamIDs(ctx context.Context, streamType string, storeStreamID string, limit int64) ([]string, string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) Close() error {
	return errors.Join(
		s.connector.Close(),
	)
}
