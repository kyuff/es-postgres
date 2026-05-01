package postgres

import "embed"

//go:embed migrations/*.sql
var DDL embed.FS
