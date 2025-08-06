package postgres

import (
	"context"
	"encoding/json"
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
	return func(yield func(es.Event, error) bool) {
		db, err := s.connector.AcquireRead(ctx)
		if err != nil {
			yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to acquire read connection: %w", err))
			return
		}
		defer db.Release()

		rows, err := s.schema.SelectEvents(ctx, db, streamType, streamID, eventNumber)
		if err != nil {
			yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to select events for %s.%s [%d]: %w",
				streamType,
				streamID,
				eventNumber,
				err,
			))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var event es.Event
			var content []byte
			var contentName string
			var metadata json.RawMessage
			err := rows.Scan(
				&event.StreamType,
				&event.StreamID,
				&event.EventNumber,
				&event.EventTime,
				&event.StoreEventID,
				&event.StoreStreamID,
				&contentName,
				&content,
				&metadata,
			)
			if err != nil {
				yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to scan events for %s.%s [%d]: %w",
					streamType,
					streamID,
					eventNumber,
					err,
				))
				return
			}

			event.Content, err = s.cfg.codec.Decode(event.StreamType, contentName, content)
			if err != nil {
				yield(es.Event{}, fmt.Errorf("[es/postgres] Failed to decode event %q for %s.%s [%d]: %w",
					contentName,
					streamType,
					streamID,
					eventNumber,
					err,
				))
				return
			}

			if !yield(event, nil) {
				return
			}
		}
	}
}

func (s *Storage) Write(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
	conn, err := s.connector.AcquireWrite(ctx)
	if err != nil {
		return fmt.Errorf("[es/postgres] Failed to acquire write connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("[es/postgres] Failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var (
		first         = true
		storeStreamID string
		streamID      string
		eventNumber   int64
	)

	for event, err := range events {
		if err != nil {
			return fmt.Errorf("[es/postgres] Range over events to be written failed: %w", err)
		}

		if first {
			first = false
			storeStreamID = event.StoreStreamID
			streamID = event.StreamID
			eventNumber = event.EventNumber - 1
		}

		if event.StreamType != streamType {
			return fmt.Errorf("[es/postgres] Invalid stream type: %q", event.StreamType)
		}
		if event.StoreStreamID != storeStreamID {
			return fmt.Errorf("[es/postgres] Invalid store stream id: %q / %q", event.StoreStreamID, storeStreamID)
		}
		if event.StreamID != streamID {
			return fmt.Errorf("[es/postgres] Invalid stream id: %q", event.StreamID)
		}

		if eventNumber+1 != event.EventNumber {
			return fmt.Errorf("[es/postgres] Invalid event number, expected %d, got %d", eventNumber, event.EventNumber)
		}

		eventNumber = event.EventNumber

		err = s.schema.WriteEvent(ctx, tx, event)
		if err != nil {
			return fmt.Errorf("[es/postgres] Failed to write event: %w", err)
		}
	}

	return tx.Commit(ctx)
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
