package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kyuff/es-postgres/internal/database/schemas"
)

func Migrate(ctx context.Context, schema *schemas.Schema, fileSystem fs.FS) error {
	migrations, err := parseSteps(fileSystem, schema.Prefix)
	if err != nil {
		return err
	}

	pid := calculateProcessID(schema.Prefix)
	err = schema.Exec(ctx, "advisory_lock.tmpl", pid)
	if err != nil {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		err := schema.Exec(unlockCtx, "advisory_unlock.tmpl", pid)
		if err != nil {
			slog.ErrorContext(unlockCtx, fmt.Sprintf("[es/postgres] Migration unlock failed: %s", err))
		}
	}()

	err = schema.Exec(ctx, "migration_table_create.tmpl")
	if err != nil {
		return err
	}

	var currentVersion uint32
	var row pgx.Row
	row, err = schema.QueryRow(ctx, "migration_table_select.tmpl")
	if err != nil {
		return err
	}
	err = row.Scan(&currentVersion)
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

		err = schema.ExecRaw(ctx, migration.ddl)
		if err != nil {
			return err
		}

		err = schema.Exec(ctx, "migration_table_insert.tmpl", migration.version, migration.fileName, migration.Hash())
		if err != nil {
			return err
		}

		currentVersion = migration.version
	}

	return nil
}
