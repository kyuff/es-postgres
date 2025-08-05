package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/assert"
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

func DSNTest(t *testing.T) string {
	t.Helper()
	var name = schemaName(t.Name())
	dsn := fmt.Sprintf("postgres://es:es@localhost:5430/es?sslmode=disable&search_path=%s", name)

	pool, err := Connect(t.Context(), dsn)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = pool.Exec(t.Context(), fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	pool.Close()

	return dsn
}
