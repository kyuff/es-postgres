package database

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/kyuff/es-postgres/internal/dbtx"
)

func Migrate(ctx context.Context, db dbtx.DBTX, schema *Schema, migrations fs.FS) error {
	steps, err := parseSteps(migrations)
	if err != nil {
		return err
	}

	pid := calculateProcessID("es_postgres_migration")
	err = schema.AdvisoryLock(ctx, db, pid)
	if err != nil {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		err := schema.AdvisoryUnlock(unlockCtx, db, pid)
		if err != nil {
			slog.ErrorContext(unlockCtx, fmt.Sprintf("migration unlock failed: %s", err))
		}
	}()

	err = schema.CreateMigrationTable(ctx, db)
	if err != nil {
		return err
	}

	currentVersion, err := schema.SelectCurrentMigration(ctx, db)
	if err != nil {
		return err
	}

	var highestVersion uint32
	for _, step := range steps {
		if step.version > highestVersion {
			highestVersion = step.version
		}
	}
	if highestVersion < currentVersion {
		return fmt.Errorf("[es/postgres] Version mismatch: current (%d) > highest (%d)", currentVersion, highestVersion)
	}

	for _, step := range steps {
		if step.version <= currentVersion {
			continue
		}

		_, err = db.Exec(ctx, step.ddl)
		if err != nil {
			return err
		}

		err = schema.InsertMigrationRow(ctx, db, step.version, step.fileName, step.Hash())
		if err != nil {
			return err
		}

		currentVersion = step.version
	}

	return nil
}
