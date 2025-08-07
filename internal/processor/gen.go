package processor

//go:generate go tool moq -pkg processor_test -rm -out mocks_test.go . Connector Reader
