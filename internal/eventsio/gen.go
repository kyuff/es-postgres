package eventsio

//go:generate go tool moq -pkg eventsio_test -rm -out mocks_test.go . Schema Validator Codec
