package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kyuff/es"
	"github.com/kyuff/es-postgres/internal/dbtx"
	"github.com/kyuff/es-postgres/internal/leases"
	"github.com/kyuff/es-postgres/internal/rfc8601"
	"github.com/kyuff/es-postgres/internal/uuid"
)

var once sync.Once
var sql = sqlQueries{}

type sqlQueries struct {
	selectCurrentMigration string
	advisoryLock           string
	advisoryUnlock         string
	createMigrationTable   string
	insertMigrationRow     string
	selectEvents           string
	writeEvent             string
	insertOutbox           string
	updateOutbox           string
	selectStreamIDs        string
	selectOutboxStreamIDs  string
	selectOutboxWatermark  string
	updateOutboxWatermark  string
	refreshLeases          string
	approveLease           string
	insertLease            string
}

func NewSchema(prefix string) (*Schema, error) {
	var err error
	once.Do(func() {
		err = renderTemplates(prefix,
			&sql.selectCurrentMigration,
			&sql.advisoryLock,
			&sql.advisoryUnlock,
			&sql.createMigrationTable,
			&sql.insertMigrationRow,
			&sql.selectEvents,
			&sql.writeEvent,
			&sql.insertOutbox,
			&sql.updateOutbox,
			&sql.selectStreamIDs,
			&sql.selectOutboxStreamIDs,
			&sql.selectOutboxWatermark,
			&sql.updateOutboxWatermark,
			&sql.refreshLeases,
			&sql.approveLease,
			&sql.insertLease,
		)
	})
	if err != nil {
		return nil, err
	}

	return &Schema{
		Prefix: prefix,
	}, nil
}

type Schema struct {
	Prefix string
}

func init() {
	sql.selectCurrentMigration = `
SELECT COALESCE(MAX(version), 0)
FROM {{ .Prefix }}_migrations;
`
}

func (s *Schema) SelectCurrentMigration(ctx context.Context, db dbtx.DBTX) (uint32, error) {
	row := db.QueryRow(ctx, sql.selectCurrentMigration)
	var current uint32
	err := row.Scan(&current)
	if err != nil {
		return current, fmt.Errorf("select current migration version: %w", err)
	}

	return current, nil
}

func init() {
	sql.advisoryLock = "SELECT pg_advisory_lock($1);"
}

func (s *Schema) AdvisoryLock(ctx context.Context, db dbtx.DBTX, pid int) error {
	_, err := db.Exec(ctx, sql.advisoryLock, pid)
	if err != nil {
		return fmt.Errorf("advisory lock %d failed: %w", pid, err)
	}

	return nil
}

func init() {
	sql.advisoryUnlock = "SELECT pg_advisory_unlock($1);"
}

func (s *Schema) AdvisoryUnlock(ctx context.Context, db dbtx.DBTX, pid int) error {
	_, err := db.Exec(ctx, sql.advisoryUnlock, pid)
	if err != nil {
		return fmt.Errorf("advisory unlock %d failed: %w", pid, err)
	}

	return nil
}

func init() {
	sql.createMigrationTable = `
CREATE TABLE IF NOT EXISTS {{ .Prefix }}_migrations
(
    version     BIGINT                      NOT NULL,
    name        VARCHAR                     NOT NULL,
    hash        VARCHAR                     NOT NULL,
    applied     timestamptz DEFAULT NOW()   NOT NULL,
    CONSTRAINT {{ .Prefix }}_migrations_pkey PRIMARY KEY (version)
);
`
}

func (s *Schema) CreateMigrationTable(ctx context.Context, db dbtx.DBTX) error {
	_, err := db.Exec(ctx, sql.createMigrationTable)
	if err != nil {
		return fmt.Errorf("create migration Table failed: %w", err)
	}

	return nil
}

func init() {
	sql.insertMigrationRow = `
INSERT INTO {{ .Prefix }}_migrations (version, name, hash)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING;
`
}

func (s *Schema) InsertMigrationRow(ctx context.Context, db dbtx.DBTX, version uint32, name string, hash string) error {
	_, err := db.Exec(ctx, sql.insertMigrationRow, version, name, hash)
	if err != nil {
		return fmt.Errorf("insert Migration row failed: %w", err)
	}

	return nil
}

func init() {
	sql.selectEvents = `
SELECT  stream_type,
        stream_id,
		event_number,
		event_time,
		store_event_id,
		store_stream_id,
		content_name,
		content,
		metadata
FROM {{ .Prefix }}_events
WHERE 
        stream_type = $1
    AND stream_id = $2
    AND event_number > $3
ORDER BY event_number ASC
`
}

func (s *Schema) SelectEvents(ctx context.Context, db dbtx.DBTX, streamType string, streamID string, eventNumber int64) (pgx.Rows, error) {
	return db.Query(ctx, sql.selectEvents, streamType, streamID, eventNumber)
}

func init() {
	sql.writeEvent = `
INSERT INTO {{ .Prefix }}_events (
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
}

func (s *Schema) WriteEvent(ctx context.Context, db dbtx.DBTX, event es.Event, content []byte, metadata []byte) error {
	_, err := db.Exec(ctx, sql.writeEvent,
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

func init() {
	sql.insertOutbox = `
INSERT INTO {{ .Prefix }}_outbox (
	stream_type,
	stream_id,
	store_stream_id,
	event_number,
	watermark,
	partition
) VALUES ($1,$2,$3,$4,$5,$6);
`
}

func (s *Schema) InsertOutbox(ctx context.Context, tx dbtx.DBTX, streamType, streamID, storeStreamID string, eventNumber, watermark int64, partition uint32) (int64, error) {
	affected, err := tx.Exec(ctx, sql.insertOutbox,
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

func init() {
	sql.updateOutbox = `
UPDATE {{ .Prefix }}_outbox
SET event_number = $3
WHERE stream_type = $1
  AND stream_id = $2
  AND event_number = $4
`
}

func (s *Schema) UpdateOutbox(ctx context.Context, tx dbtx.DBTX, streamType, streamID string, eventNumber, lastEventNumber int64) (int64, error) {
	affected, err := tx.Exec(ctx, sql.updateOutbox, streamType, streamID, eventNumber, lastEventNumber)
	if err != nil {
		return 0, err
	}

	return affected.RowsAffected(), nil
}

func init() {
	sql.selectStreamIDs = `
SELECT stream_id, store_stream_id
FROM {{ .Prefix }}_outbox
WHERE store_stream_id > $1
  AND stream_type = $2
ORDER BY store_stream_id ASC
LIMIT $3;`
}

func (s *Schema) SelectStreamIDs(ctx context.Context, db dbtx.DBTX, streamType string, token string, limit int64) ([]string, string, error) {
	if token == "" {
		token = uuid.Empty
	}
	rows, err := db.Query(ctx, sql.selectStreamIDs, token, streamType, limit)
	if err != nil {
		return nil, "", err
	}

	var result []string
	var nextToken = ""
	for rows.Next() {
		var streamID string
		err = rows.Scan(&streamID, &nextToken)
		if err != nil {
			return nil, "", err
		}

		result = append(result, streamID)
	}

	if len(result) == 0 {
		nextToken = token
	}

	return result, nextToken, nil
}

func init() {
	sql.selectOutboxStreamIDs = `
SELECT stream_type,
       store_stream_id
FROM {{ .Prefix }}_outbox
WHERE
	 watermark <> event_number 
 AND partition = ANY ($1)
 AND store_stream_id > $2
 AND process_at <= $3
ORDER BY store_stream_id
LIMIT $4    
`
}

func (s *Schema) SelectOutboxStreamIDs(ctx context.Context, db dbtx.DBTX, graceWindow time.Duration, partitions []uint32, token string, limit int) ([]Stream, error) {
	if token == "" {
		token = uuid.Empty
	}
	rows, err := db.Query(ctx, sql.selectOutboxStreamIDs,
		partitions,
		token,
		time.Now().Add(-graceWindow),
		limit,
	)
	if err != nil {
		return nil, err
	}

	var result []Stream
	for rows.Next() {
		var stream Stream
		err = rows.Scan(&stream.Type, &stream.StoreID)
		if err != nil {
			return nil, err
		}

		result = append(result, stream)
	}

	return result, nil
}

func init() {
	sql.selectOutboxWatermark = `
SELECT 
    stream_id,
	event_number,
	watermark,
	retry_count
FROM {{ .Prefix }}_outbox
WHERE
    stream_type = $1
AND store_stream_id = $2;
`
}
func (s *Schema) SelectOutboxWatermark(ctx context.Context, db dbtx.DBTX, stream Stream) (OutboxWatermark, int64, error) {
	var (
		row         = db.QueryRow(ctx, sql.selectOutboxWatermark, stream.Type, stream.StoreID)
		w           OutboxWatermark
		eventNumber int64
	)

	err := row.Scan(&w.StreamID, &eventNumber, &w.Watermark, &w.RetryCount)
	return w, eventNumber, err
}

func init() {
	sql.updateOutboxWatermark = `
UPDATE {{ .Prefix }}_outbox
SET 
	watermark = $4,
    retry_count = $5,
    process_at = $3
WHERE stream_type = $1
  AND store_stream_id = $2
`
}
func (s *Schema) UpdateOutboxWatermark(ctx context.Context, db dbtx.DBTX, stream Stream, delay time.Duration, watermark OutboxWatermark) error {
	tag, err := db.Exec(ctx, sql.updateOutboxWatermark,
		stream.Type,
		stream.StoreID,
		time.Now().Add(delay),
		watermark.Watermark,
		watermark.RetryCount,
	)
	if err != nil {
		return err
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf("stream %q not watermark updated", stream.Type)
	}
	return err
}

func init() {
	sql.refreshLeases = `
WITH
	evacuate AS (
         DELETE FROM {{ .Prefix }}_leases
         WHERE ttl < NOW()
    ),
    refresh AS (
		UPDATE {{ .Prefix }}_leases
        SET ttl = NOW() + $2::interval
        WHERE node_name = $1
        RETURNING *
    )
SELECT l.vnode, 
       l.node_name, 
       r.ttl > NOW(),
       l.status
FROM {{ .Prefix }}_leases AS l
    LEFT JOIN refresh AS r USING (vnode)
WHERE l.ttl >= NOW()
ORDER BY vnode ASC;
`
}

func (s *Schema) RefreshLeases(ctx context.Context, db dbtx.DBTX, nodeName string, ttl time.Duration) (leases.Ring, error) {
	rows, err := db.Query(ctx, sql.refreshLeases, nodeName, rfc8601.Format(ttl))
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

func init() {
	sql.approveLease = `
UPDATE {{ .Prefix }}_leases
SET
  status = $2
WHERE
  vnode = ANY ($1);
`
}

func (s *Schema) ApproveLease(ctx context.Context, db dbtx.DBTX, vnodes []uint32) error {
	tag, err := db.Exec(ctx, sql.approveLease, vnodes, leases.Leased)
	if err != nil {
		return err
	}

	if len(vnodes) != int(tag.RowsAffected()) {
		return fmt.Errorf("leases approval mismatch: %d != %d", len(vnodes), tag.RowsAffected())
	}

	return nil
}

func init() {
	sql.insertLease = `
INSERT INTO {{ .Prefix }}_leases
(vnode, node_name, ttl, status)
VALUES ($1, $2, NOW() + $3::interval, $4);
`
}
func (s *Schema) InsertLease(ctx context.Context, db dbtx.DBTX, vnode uint32, name string, ttl time.Duration, status string) error {
	_, err := db.Exec(ctx, sql.insertLease, vnode, name, rfc8601.Format(ttl), status)
	return err
}
