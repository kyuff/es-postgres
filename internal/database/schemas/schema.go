package schemas

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"text/template"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sql/*.tmpl
var sqlFiles embed.FS

func New(pool *pgxpool.Pool, prefix string) (*Schema, error) {
	tmpl, err := template.ParseFS(sqlFiles, "**/*.tmpl")
	if err != nil {
		return nil, err
	}

	var params = struct {
		Prefix string
	}{
		Prefix: prefix,
	}
	sql := make(map[string]string)
	for _, t := range tmpl.Templates() {
		var buf bytes.Buffer
		err = t.Execute(&buf, params)
		if err != nil {
			return nil, fmt.Errorf("error executing template %s: %w", t.Name(), err)
		}
		sql[t.Name()] = buf.String()
	}

	fmt.Printf("SQL:\n%#v\n", sql)
	return &Schema{
		Prefix: prefix,
		sql:    sql,
		pool:   pool,
	}, nil
}

type Schema struct {
	Prefix string
	sql    map[string]string
	pool   *pgxpool.Pool
}

func (s *Schema) ExecRaw(ctx context.Context, query string, args ...any) error {
	_, err := s.pool.Exec(ctx, query, args...)
	return err
}

func (s *Schema) Exec(ctx context.Context, file string, args ...any) error {
	query, ok := s.sql[file]
	if !ok {
		return fmt.Errorf("sql file %q not found", file)
	}

	_, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query %s failed: %w", file, err)
	}

	return nil
}

func (s *Schema) QueryRow(ctx context.Context, file string, args ...any) (pgx.Row, error) {
	query, ok := s.sql[file]
	if !ok {
		return nil, fmt.Errorf("sql file %q not found", file)
	}

	return s.pool.QueryRow(ctx, query, args...), nil
}
