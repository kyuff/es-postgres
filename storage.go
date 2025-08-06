package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/database"
	"golang.org/x/sync/errgroup"
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

	valuer := newFixedSizeValuer(128) // TODO Option for size

	return &Storage{
		cfg:       cfg,
		connector: connector,
		schema:    schema,
		reconciles: []reconcile{
			newReconcileListener(),
			newReconcilePeriodic(cfg, connector, schema, valuer),
		},
	}, nil
}

type reconcile interface {
	Reconcile(ctx context.Context, p processor) error
}

type Storage struct {
	cfg        *Config
	connector  Connector
	schema     *database.Schema
	reconciles []reconcile
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

	err = s.writeEvents(ctx, tx, streamType, events)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Storage) writeEvents(ctx context.Context, tx database.DBTX, streamType string, events iter.Seq2[es.Event, error]) error {
	var (
		firstEvent es.Event
		lastEvent  es.Event
		eventCount = 0
	)

	for event, err := range validateStreamWrite(streamType, events) {
		if err != nil {
			return fmt.Errorf("[es/postgres] Range over events to be written failed: %w", err)
		}

		if eventCount == 0 {
			firstEvent = event
		}

		err = s.schema.WriteEvent(ctx, tx, event)
		if err != nil {
			return fmt.Errorf("[es/postgres] Failed to write event: %w", err)
		}

		lastEvent = event
		eventCount++
	}

	if eventCount == 0 {
		return nil // nothing was done
	}

	var affected int64
	var err error
	if firstEvent.EventNumber == 1 {
		affected, err = s.schema.InsertOutbox(ctx, tx,
			streamType,
			lastEvent.StreamID,
			lastEvent.StoreStreamID,
			lastEvent.EventNumber,
			firstEvent.EventNumber-1,
			s.cfg.partitioner(streamType, lastEvent.StreamID),
		)
	} else {
		affected, err = s.schema.UpdateOutbox(ctx, tx,
			streamType,
			lastEvent.StreamID,
			lastEvent.EventNumber,
			firstEvent.EventNumber-1,
		)
	}
	if err != nil {
		return err
	}

	if affected != 1 {
		return fmt.Errorf("[es/postgres] Failed to update outbox for %s.%s", streamType, lastEvent.StreamID)
	}

	return nil
}

func (s *Storage) StartPublish(ctx context.Context, w es.Writer) error {
	g, publishCtx := errgroup.WithContext(ctx)

	p := newProcessWriter(w)
	for _, r := range s.reconciles {
		g.Go(func() error {
			return r.Reconcile(publishCtx, p)
		})
	}

	return g.Wait()
}

func (s *Storage) Register(streamType string, types ...es.Content) error {
	return s.cfg.codec.Register(streamType, types...)
}

func (s *Storage) GetStreamIDs(ctx context.Context, streamType string, storeStreamID string, limit int64) ([]string, string, error) {
	db, err := s.connector.AcquireRead(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("[es/postgres] Failed to acquire read connection: %w", err)
	}
	defer db.Release()

	if strings.TrimSpace(storeStreamID) == "" {
		storeStreamID = "00000000-0000-0000-0000-000000000000"
	}

	streamIDs, nextToken, err := s.schema.SelectStreamIDs(ctx, db, streamType, storeStreamID, limit)
	if err != nil {
		return nil, "", fmt.Errorf("[es/postgres] Failed to read store stream ids: %w", err)
	}

	return streamIDs, nextToken, nil
}

func (s *Storage) Close() error {
	return errors.Join(
		s.connector.Close(),
	)
}
