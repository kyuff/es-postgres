package reconcilers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es"
	"golang.org/x/sync/errgroup"
)

func NewListener(connector Connector, channelPrefix string) *Listener {
	return &Listener{
		connector: connector,
	}
}

type Listener struct {
	connector      Connector
	processTimeout time.Duration
	channelPrefix  string
	logger         Logger

	mu     sync.RWMutex
	conn   *pgxpool.Conn
	values []uint32
}

func (l *Listener) Reconcile(ctx context.Context, p Processor) error {

	for {
		err := l.process(ctx, p)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}

		if err != nil {
			l.logger.ErrorfCtx(ctx, "[es/postgres] Reconciliation error: %v", err)
		}
	}

}

func (l *Listener) process(ctx context.Context, p Processor) error {
	var err error
	l.conn, err = l.connector.AcquireRead(ctx)
	if err != nil {
		return err
	}
	defer func() {
		l.conn.Release()
		l.conn = nil
	}()

	l.mu.RLock()
	err = l.listen(ctx, l.values)
	l.mu.RUnlock()
	if err != nil {
		return err
	}

	var g errgroup.Group
	for {
		notification, waitErr := l.conn.Conn().WaitForNotification(ctx)
		if waitErr != nil {
			l.logger.ErrorfCtx(ctx, "[es/postgres] Reconciliation Listen Wait error: %v", waitErr)
			err = errors.Join(err, waitErr)
			break
		}

		g.Go(func() error {
			stream, err := parseStreamReference(notification.Payload)
			if err != nil {
				return err
			}

			processCtx, processCancel := context.WithTimeout(ctx, l.processTimeout)
			defer processCancel()

			return p.Process(processCtx, stream)
		})
	}

	return errors.Join(err, g.Wait())
}

func (l *Listener) ValuesChanged(ctx context.Context, values, added, removed []uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.values = values

	return errors.Join(
		l.listen(ctx, added),
		l.unlisten(ctx, removed),
	)
}

func (l *Listener) listen(ctx context.Context, partitions []uint32) error {
	if l.conn == nil {
		return fmt.Errorf("no connection")
	}

	for _, partition := range partitions {
		_, err := l.conn.Exec(ctx, "LISTEN "+channelName(l.channelPrefix, partition))
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *Listener) unlisten(ctx context.Context, partitions []uint32) error {
	if l.conn == nil {
		return fmt.Errorf("no connection")
	}

	for _, partition := range partitions {
		_, err := l.conn.Exec(ctx, "UNLISTEN "+channelName(l.channelPrefix, partition))
		if err != nil {
			return err
		}
	}

	return nil
}

func channelName(prefix string, partition uint32) string {
	return fmt.Sprintf("%s_%d", prefix, partition)
}

func parseStreamReference(payload string) (es.StreamReference, error) {
	return es.StreamReference{
		StreamType:    "",
		StreamID:      "",
		StoreStreamID: "",
	}, nil
}
