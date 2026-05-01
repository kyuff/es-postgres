package database

import (
	"crypto/sha512"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
)

type step struct {
	version  uint32
	fileName string
	ddl      string
}

func (s step) Hash() string {
	return fmt.Sprintf("%x", sha512.Sum512([]byte(s.ddl)))
}

func parseSteps(fileSystem fs.FS) ([]step, error) {
	entries, err := fs.Glob(fileSystem, "migrations/*.sql")
	if err != nil {
		return nil, err
	}

	slices.Sort(entries)

	var migrations []step
	for _, entry := range entries {
		data, err := fs.ReadFile(fileSystem, entry)
		if err != nil {
			return nil, err
		}

		version, err := extractVersionNumber(path.Base(entry))
		if err != nil {
			return nil, err
		}

		migrations = append(migrations, step{
			version:  version,
			fileName: entry,
			ddl:      string(data),
		})
	}

	for i := 0; i < len(migrations); i++ {
		if uint32(i+1) != migrations[i].version {
			return nil, fmt.Errorf("wrong sequence: %s", migrations[i].fileName)
		}
	}

	return migrations, nil
}

func extractVersionNumber(name string) (uint32, error) {
	if len(name) < 4 {
		return 0, fmt.Errorf("file name too short: %s", name)
	}

	n, err := strconv.Atoi(name[0:3])
	if err != nil {
		return 0, fmt.Errorf("file name must start with numbers %q: %s", name, err)
	}

	return uint32(n), nil
}
