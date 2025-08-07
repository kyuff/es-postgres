package processor

import "github.com/kyuff/es"

//go:generate go tool moq -pkg processor_test -rm -out mocks_test.go . Connector Reader Schema Writer

type Writer es.Writer
