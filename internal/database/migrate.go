package database

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"time"
)

func Migrate(ctx context.Context, schema *Schema, fileSystem fs.FS) error {
	migrations, err := parseSteps(fileSystem, schema.Prefix)
	if err != nil {
		return err
	}

	pid := calculateProcessID(schema.Prefix)
	err = schema.AdvisoryLock(ctx, pid)
	if err != nil {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		err := schema.AdvisoryUnlock(unlockCtx, pid)
		if err != nil {
			slog.ErrorContext(unlockCtx, fmt.Sprintf("[es/postgres] Migration unlock failed: %s", err))
		}
	}()

	err = schema.CreateMigrationTable(ctx)
	if err != nil {
		return err
	}

	currentVersion, err := schema.SelectCurrentMigration(ctx)
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

		err = schema.Exec(ctx, migration.ddl)
		if err != nil {
			return err
		}

		err = schema.InsertMigrationRow(ctx, migration.version, migration.fileName, migration.Hash())
		if err != nil {
			return err
		}

		currentVersion = migration.version
	}

	return nil
}
