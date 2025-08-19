package reconcilers

import "context"

func NewListener() *Listener {
	return &Listener{}
}

type Listener struct{}

func (l *Listener) Reconcile(ctx context.Context, p Processor) error {
	return nil
}

func (l *Listener) ValuesChanged(ctx context.Context, added, removed []uint32) error {
	return nil
}
