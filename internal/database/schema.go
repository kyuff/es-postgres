package database

import (
	"context"
	"fmt"
)

var sql = sqlQueries{}

type sqlQueries struct {
	selectCurrentMigration string
	advisoryLock           string
	advisoryUnlock         string
	createMigrationTable   string
	insertMigrationRow     string
}

func NewSchema(prefix string) (*Schema, error) {
	err := renderTemplates(prefix,
		&sql.selectCurrentMigration,
		&sql.advisoryLock,
		&sql.advisoryUnlock,
		&sql.createMigrationTable,
		&sql.insertMigrationRow,
	)
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

func (s *Schema) SelectCurrentMigration(ctx context.Context, db DBTX) (uint32, error) {
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

func (s *Schema) AdvisoryLock(ctx context.Context, db DBTX, pid int) error {
	_, err := db.Exec(ctx, sql.advisoryLock, pid)
	if err != nil {
		return fmt.Errorf("advisory lock %d failed: %w", pid, err)
	}

	return nil
}

func init() {
	sql.advisoryUnlock = "SELECT pg_advisory_unlock($1);"
}

func (s *Schema) AdvisoryUnlock(ctx context.Context, db DBTX, pid int) error {
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

func (s *Schema) CreateMigrationTable(ctx context.Context, db DBTX) error {
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

func (s *Schema) InsertMigrationRow(ctx context.Context, db DBTX, version uint32, name string, hash string) error {
	_, err := db.Exec(ctx, sql.insertMigrationRow, version, name, hash)
	if err != nil {
		return fmt.Errorf("insert Migration row failed: %w", err)
	}

	return nil
}
