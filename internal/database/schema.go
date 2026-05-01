package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/leases"
	"github.com/kyuff/es-postgres/internal/rfc8601"
	"github.com/kyuff/es-postgres/internal/uuid"
)

func NewSchema(notifyPrefix string) *Schema {
	return &Schema{
		notifyPrefix: notifyPrefix,
	}
}

type Schema struct {
	notifyPrefix string
}

func (s *Schema) SelectCurrentMigration(ctx context.Context, db dbtx.DBTX) (uint32, error) {
	const q = `
SELECT COALESCE(MAX(version), 0)
FROM es_migrations;
`
	row := db.QueryRow(ctx, q)
	var current uint32
	err := row.Scan(&current)
	if err != nil {
		return current, fmt.Errorf("select current migration version: %w", err)
	}

	return current, nil
}

func (s *Schema) AdvisoryLock(ctx context.Context, db dbtx.DBTX, pid int) error {
	_, err := db.Exec(ctx, "SELECT pg_advisory_lock($1);", pid)
	if err != nil {
		return fmt.Errorf("advisory lock %d failed: %w", pid, err)
	}

	return nil
}

func (s *Schema) AdvisoryUnlock(ctx context.Context, db dbtx.DBTX, pid int) error {
	_, err := db.Exec(ctx, "SELECT pg_advisory_unlock($1);", pid)
	if err != nil {
		return fmt.Errorf("advisory unlock %d failed: %w", pid, err)
	}

	return nil
}

func (s *Schema) CreateMigrationTable(ctx context.Context, db dbtx.DBTX) error {
	const q = `
CREATE TABLE IF NOT EXISTS es_migrations
(
    version     BIGINT                      NOT NULL,
    name        VARCHAR                     NOT NULL,
    hash        VARCHAR                     NOT NULL,
    applied     timestamptz DEFAULT NOW()   NOT NULL,
    CONSTRAINT es_migrations_pkey PRIMARY KEY (version)
);
`
	_, err := db.Exec(ctx, q)
	if err != nil {
		return fmt.Errorf("create migration Table failed: %w", err)
	}

	return nil
}

func (s *Schema) InsertMigrationRow(ctx context.Context, db dbtx.DBTX, version uint32, name string, hash string) error {
	const q = `
INSERT INTO es_migrations (version, name, hash)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING;
`
	_, err := db.Exec(ctx, q, version, name, hash)
	if err != nil {
		return fmt.Errorf("insert Migration row failed: %w", err)
	}

	return nil
}

func (s *Schema) SelectEvents(ctx context.Context, db dbtx.DBTX, streamType string, streamID string, eventNumber int64) (pgx.Rows, error) {
	const q = `
SELECT  stream_type,
        stream_id,
		event_number,
		event_time,
		store_event_id,
		store_stream_id,
		content_name,
		content,
		metadata
FROM es_events
WHERE
        stream_type = $1
    AND stream_id = $2
    AND event_number > $3
ORDER BY event_number ASC
`
	return db.Query(ctx, q, streamType, streamID, eventNumber)
}

func (s *Schema) WriteEvent(ctx context.Context, db dbtx.DBTX, event es.Event, content []byte, metadata []byte) error {
	const q = `
INSERT INTO es_events (
                                stream_type,
                                stream_id,
                                event_number,
                                event_time,
                                store_event_id,
                                store_stream_id,
                                content_name,
                                content,
                                metadata
                            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9);`
	_, err := db.Exec(ctx, q,
		event.StreamType,
		event.StreamID,
		event.EventNumber,
		event.EventTime,
		event.StoreEventID,
		event.StoreStreamID,
		event.Content.EventName(),
		content,
		`{}`,
	)
	return err
}

func (s *Schema) InsertOutbox(ctx context.Context, tx dbtx.DBTX, streamType, streamID, storeStreamID string, eventNumber, watermark int64, partition uint32) (int64, error) {
	const q = `
INSERT INTO es_outbox (
	stream_type,
	stream_id,
	store_stream_id,
	event_number,
	watermark,
	partition
) VALUES ($1,$2,$3,$4,$5,$6);
`
	affected, err := tx.Exec(ctx, q,
		streamType,
		streamID,
		storeStreamID,
		eventNumber,
		watermark,
		partition,
	)
	if err != nil {
		return 0, err
	}

	return affected.RowsAffected(), nil
}

func (s *Schema) UpdateOutbox(ctx context.Context, tx dbtx.DBTX, streamType, streamID string, eventNumber, lastEventNumber int64) (int64, error) {
	const q = `
UPDATE es_outbox
SET event_number = $3
WHERE stream_type = $1
  AND stream_id = $2
  AND event_number = $4
`
	affected, err := tx.Exec(ctx, q, streamType, streamID, eventNumber, lastEventNumber)
	if err != nil {
		return 0, err
	}

	return affected.RowsAffected(), nil
}

func (s *Schema) SelectStreamReferences(ctx context.Context, db dbtx.DBTX, streamType string, token string, limit int64) (pgx.Rows, error) {
	const q = `
SELECT stream_type, stream_id, store_stream_id
FROM es_outbox
WHERE store_stream_id > $1
  AND stream_type = $2
ORDER BY store_stream_id ASC
LIMIT $3;`
	return db.Query(ctx, q, token, streamType, limit)
}

func (s *Schema) SelectOutboxStreamIDs(ctx context.Context, db dbtx.DBTX, graceWindow time.Duration, partitions []uint32, token string, limit int) ([]es.StreamReference, error) {
	const q = `
SELECT stream_type,
       stream_id,
       store_stream_id
FROM es_outbox
WHERE
	 watermark <> event_number
 AND partition = ANY ($1)
 AND store_stream_id > $2
 AND process_at <= $3
ORDER BY store_stream_id
LIMIT $4
`
	if token == "" {
		token = uuid.Empty
	}
	rows, err := db.Query(ctx, q,
		partitions,
		token,
		time.Now().Add(-graceWindow),
		limit,
	)
	if err != nil {
		return nil, err
	}

	var result []es.StreamReference
	for rows.Next() {
		var stream es.StreamReference
		err = rows.Scan(&stream.StreamType, &stream.StreamID, &stream.StoreStreamID)
		if err != nil {
			return nil, err
		}

		result = append(result, stream)
	}

	return result, nil
}

func (s *Schema) SelectOutboxWatermark(ctx context.Context, db dbtx.DBTX, stream es.StreamReference) (OutboxWatermark, int64, error) {
	const q = `
SELECT
    stream_id,
	event_number,
	watermark,
	retry_count
FROM es_outbox
WHERE
    stream_type = $1
AND store_stream_id = $2;
`
	var (
		row         = db.QueryRow(ctx, q, stream.StreamType, stream.StoreStreamID)
		w           OutboxWatermark
		eventNumber int64
	)

	err := row.Scan(&w.StreamID, &eventNumber, &w.Watermark, &w.RetryCount)
	return w, eventNumber, err
}

func (s *Schema) UpdateOutboxWatermark(ctx context.Context, db dbtx.DBTX, stream es.StreamReference, delay time.Duration, watermark OutboxWatermark) error {
	const q = `
UPDATE es_outbox
SET
	watermark = $4,
    retry_count = $5,
    process_at = $3
WHERE stream_type = $1
  AND store_stream_id = $2
`
	tag, err := db.Exec(ctx, q,
		stream.StreamType,
		stream.StoreStreamID,
		time.Now().Add(delay),
		watermark.Watermark,
		watermark.RetryCount,
	)
	if err != nil {
		return err
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf("stream %q not watermark updated", stream.StreamType)
	}
	return err
}

func (s *Schema) RefreshLeases(ctx context.Context, db dbtx.DBTX, nodeName string, ttl time.Duration) (leases.Ring, error) {
	const evacuate = `DELETE FROM es_leases WHERE ttl < NOW()`
	const update = `UPDATE es_leases SET ttl = NOW() + $2::interval WHERE node_name = $1`
	const sel = `
SELECT vnode, node_name, ttl > NOW() AS valid, status
FROM es_leases
ORDER BY vnode ASC;
`
	_, err := db.Exec(ctx, evacuate)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(ctx, update, nodeName, rfc8601.Format(ttl))
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sel)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ring leases.Ring
	for rows.Next() {
		var info leases.VNode
		err = rows.Scan(
			&info.VNode,
			&info.NodeName,
			&info.Valid,
			&info.Status,
		)
		if err != nil {
			return nil, err
		}
		ring = append(ring, info)
	}

	return ring, nil
}

func (s *Schema) ApproveLease(ctx context.Context, db dbtx.DBTX, vnodes []uint32) error {
	const q = `
UPDATE es_leases
SET
  status = $2
WHERE
  vnode = ANY ($1);
`
	tag, err := db.Exec(ctx, q, vnodes, leases.Leased)
	if err != nil {
		return err
	}

	if len(vnodes) != int(tag.RowsAffected()) {
		return fmt.Errorf("leases approval mismatch: %d != %d", len(vnodes), tag.RowsAffected())
	}

	return nil
}

func (s *Schema) InsertLease(ctx context.Context, db dbtx.DBTX, vnode uint32, name string, ttl time.Duration, status string) error {
	const q = `
INSERT INTO es_leases
(vnode, node_name, ttl, status)
VALUES ($1, $2, NOW() + $3::interval, $4);
`
	_, err := db.Exec(ctx, q, vnode, name, rfc8601.Format(ttl), status)
	return err
}

func (s *Schema) Notify(ctx context.Context, db dbtx.DBTX, partition uint32, payload string) error {
	query := fmt.Sprintf("NOTIFY %s, '%s'", s.channelName(partition), payload)
	_, err := db.Exec(ctx, query)
	return err
}

func (s *Schema) Listen(ctx context.Context, db dbtx.DBTX, partitions []uint32) error {
	if db == nil {
		return fmt.Errorf("no connection")
	}

	for _, partition := range partitions {
		_, err := db.Exec(ctx, "LISTEN "+s.channelName(partition))
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Schema) Unlisten(ctx context.Context, db dbtx.DBTX, partitions []uint32) error {
	if db == nil {
		return fmt.Errorf("no connection")
	}

	for _, partition := range partitions {
		_, err := db.Exec(ctx, "UNLISTEN "+s.channelName(partition))
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Schema) channelName(partition uint32) string {
	return fmt.Sprintf("%s_es_%d", s.notifyPrefix, partition)
}
