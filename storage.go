package postgres

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/database"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/eventsio"
	"github.com/kyuff/es-postgres/internal/leases"
	"github.com/kyuff/es-postgres/internal/processor"
	"github.com/kyuff/es-postgres/internal/reconcilers"
	"github.com/kyuff/es-postgres/internal/uuid"
	"golang.org/x/sync/errgroup"
)

type reader interface {
	Read(ctx context.Context, streamType, streamID string, eventNumber int64) iter.Seq2[es.Event, error]
}

type writer interface {
	Write(ctx context.Context, db dbtx.DBTX, streamType string, events iter.Seq2[es.Event, error]) error
}

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

	var recons []reconcilers.Reconciler
	if cfg.listenPublishing {
		var listener = reconcilers.NewListener(connector, schema, cfg.logger, cfg.processTimeout)
		cfg.leasesOptions = append(cfg.leasesOptions, leases.WithValueListener(listener))
		recons = append(recons, listener)
	}

	valuer, err := leases.New(connector, schema, cfg.leasesOptions...)
	if err != nil {
		return nil, err
	}

	if cfg.reconcilePublishing {
		recons = append(recons, reconcilers.NewPeriodic(
			cfg.logger,
			connector,
			schema,
			valuer,
			cfg.reconcileInterval,
			cfg.reconcileTimeout,
			cfg.processTimeout,
		))
	}

	return &Storage{
		cfg:        cfg,
		connector:  connector,
		schema:     schema,
		leases:     valuer,
		reader:     eventsio.NewReader(connector, schema, cfg.codec),
		writer:     eventsio.NewWriter(schema, eventsio.NewValidator(), cfg.codec, cfg.partitioner),
		reconciles: recons,
	}, nil
}

var _ es.Storage = (*Storage)(nil)

type Storage struct {
	cfg        *Config
	connector  Connector
	schema     *database.Schema
	leases     *leases.Leases
	reader     reader
	writer     writer
	reconciles []reconcilers.Reconciler
}

func (s *Storage) Read(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[es.Event, error] {
	return s.reader.Read(ctx, streamType, streamID, eventNumber)
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

	err = s.writer.Write(ctx, tx, streamType, events)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Storage) StartPublish(ctx context.Context, w es.Writer) error {
	g, publishCtx := errgroup.WithContext(ctx)

	p := processor.New(s.connector, s.schema, w, s.reader, s.cfg.processBackoff)

	g.Go(func() error {
		return s.leases.Start(ctx)
	})

	for _, r := range s.reconciles {
		fmt.Printf("STARTING RECONCILER: %T\n", r)
		g.Go(func() error {
			return r.Reconcile(publishCtx, p)
		})
	}

	return g.Wait()
}

func (s *Storage) Register(streamType string, types ...es.Content) error {
	return s.cfg.codec.Register(streamType, types...)
}

func (s *Storage) GetStreamReferences(ctx context.Context, streamType string, storeStreamID string, limit int64) iter.Seq2[es.StreamReference, error] {
	return func(yield func(es.StreamReference, error) bool) {
		db, err := s.connector.AcquireRead(ctx)
		if err != nil {
			yield(es.StreamReference{}, fmt.Errorf("[es/postgres] Failed to acquire read connection: %w", err))
			return
		}
		defer db.Release()

		if strings.TrimSpace(storeStreamID) == "" {
			storeStreamID = uuid.Empty
		}

		rows, err := s.schema.SelectStreamReferences(ctx, db, streamType, storeStreamID, limit)
		if err != nil {
			yield(es.StreamReference{}, fmt.Errorf("[es/postgres] Failed to read stream references: %w", err))
			return
		}

		for rows.Next() {
			var ref es.StreamReference
			err = rows.Scan(&ref.StreamType, &ref.StreamID, &ref.StoreStreamID)
			if err != nil {
				yield(es.StreamReference{}, fmt.Errorf("[es/postgres] Failed to scan stream references: %w", err))
				return
			}

			if !yield(ref, nil) {
				return
			}
		}

	}
}

func (s *Storage) Close() error {
	return errors.Join(
		s.connector.Close(),
	)
}
