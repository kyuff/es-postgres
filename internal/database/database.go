package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/assert"

	_ "github.com/jackc/pgx/v5"
)

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	// Optional tuning
	config.MaxConns = 10
	config.MaxConnLifetime = time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	err = pool.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func ConnectTest(t *testing.T) *pgxpool.Pool {
	t.Helper()
	var schema = schemaName(t.Name())
	var dsn = fmt.Sprintf("postgres://es:es@localhost:5430/es?sslmode=disable&search_path=%s", schema)

	pool, err := Connect(t.Context(), dsn)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = pool.Exec(t.Context(), fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, schema))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Logf("Using schema: %s", schema)

	return pool
}
