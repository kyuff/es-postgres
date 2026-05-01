package database

import (
	"context"

	"github.com/kyuff/es-postgres/internal/dbtx"
)

func CurrentSchema(ctx context.Context, db dbtx.DBTX) (string, error) {
	var name string
	err := db.QueryRow(ctx, "SELECT current_schema()").Scan(&name)
	return name, err
}
