package database

import (
	"context"
	"fmt"
	"strings"
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

	t.Logf("Using Schema %s", name)

	return dsn
}

func ToDSN(conn *pgxpool.Conn) string {
	cfg := conn.Conn().Config()
	sb := strings.Builder{}
	_, _ = fmt.Fprintf(&sb, "%s:******@%s:%d/%s",
		cfg.User,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)
	if len(cfg.RuntimeParams) == 0 {
		return sb.String()
	}

	sb.WriteString("?")

	for key, value := range cfg.RuntimeParams {
		sb.WriteString(fmt.Sprintf("&%s=%s", key, value))
	}

	return sb.String()
}
