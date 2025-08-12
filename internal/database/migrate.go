package database

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"time"

	"github.com/kyuff/es-postgres/internal/dbtx"
)

//go:embed migrations/*.tmpl
var migrations embed.FS

func Migrate(ctx context.Context, db dbtx.DBTX, schema *Schema) error {
	migrations, err := parseSteps(migrations, schema.Prefix)
	if err != nil {
		return err
	}

	pid := calculateProcessID(schema.Prefix)
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

	// first check that current version is <= the last of the available migrations. If not, something's seriously wrong
	var highestVersion uint32
	for _, migration := range migrations {
		if migration.version > highestVersion {
			highestVersion = migration.version
		}
	}
	if highestVersion < currentVersion {
		return fmt.Errorf("[es/postgres] Version mismatch: current (%d) > highest (%d)", currentVersion, highestVersion)
	}

	for _, migration := range migrations {
		if migration.version <= currentVersion {
			continue
		}

		_, err = db.Exec(ctx, migration.ddl)
		if err != nil {
			return err
		}

		err = schema.InsertMigrationRow(ctx, db, migration.version, migration.fileName, migration.Hash())
		if err != nil {
			return err
		}

		currentVersion = migration.version
	}

	return nil
}
