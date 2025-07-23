package migrate

import (
	"bytes"
	"crypto/sha512"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"text/template"
)

type step struct {
	version  uint32
	fileName string
	ddl      string
}
type inputParams struct {
	Prefix string
}

func (s step) Hash() string {
	return fmt.Sprintf("%x", sha512.Sum512([]byte(s.ddl)))
}

func parseSteps(fileSystem fs.FS, prefix string) ([]step, error) {
	archive, err := template.ParseFS(fileSystem, "**/*.tmpl")
	if err != nil {
		return nil, err
	}

	templates := archive.Templates()
	slices.SortFunc(templates, func(a, b *template.Template) int {
		if a.Name() < b.Name() {
			return -1
		}
		if a.Name() > b.Name() {
			return 1
		}

		return 0
	})

	var params = inputParams{
		Prefix: prefix,
	}
	var migrations []step
	for _, t := range templates {
		var buf bytes.Buffer
		err := t.Execute(&buf, params)
		if err != nil {
			return nil, err
		}

		version, err := extractVersionNumber(t.Name())
		if err != nil {
			return nil, err
		}

		migrations = append(migrations, step{
			version:  version,
			fileName: t.Name(),
			ddl:      buf.String(),
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
