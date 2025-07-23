package postgres

import (
	"context"
	"iter"

	"github.com/kyuff/es"
)

type Storage struct {
}

func (s *Storage) Read(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[es.Event, error] {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) Write(ctx context.Context, streamType string, events iter.Seq2[es.Event, error]) error {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) StartPublish(ctx context.Context, w es.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) Register(streamType string, types ...es.Content) error {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) GetStreamIDs(ctx context.Context, streamType string, storeStreamID string, limit int64) ([]string, string, error) {
	//TODO implement me
	panic("implement me")
}
