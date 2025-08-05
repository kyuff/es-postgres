package postgres

import "github.com/kyuff/es"

type codec interface {
	Encode(event es.Event) ([]byte, error)
	Decode(streamType, contentName string, b []byte) (es.Content, error)
	Register(streamType string, contentTypes ...es.Content) error
}
