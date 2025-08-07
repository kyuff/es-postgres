package postgres

import "github.com/kyuff/es"

//go:generate go tool moq -pkg postgres_test -rm -out mocks_test.go . Writer

type Writer es.Writer
