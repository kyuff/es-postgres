package reconcilers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kyuff/es-postgres/internal/listenerpayload"
)

func NewListener(connector Connector, schema Schema, logger Logger, timeout time.Duration) *Listener {
	l := &Listener{
		connector:      connector,
		schema:         schema,
		logger:         logger,
		processTimeout: timeout,

		commands: make(chan valueCommand, 10),
		cancel:   nil,
	}
	l.ready = sync.NewCond(&l.mu)
	return l
}

type Listener struct {
	connector      Connector
	processTimeout time.Duration
	schema         Schema
	logger         Logger
	commands       chan valueCommand
	ready          *sync.Cond
	started        bool
	mu             sync.Mutex
	cancel         context.CancelFunc // cancels the current WaitForNotification
}

func (l *Listener) Reconcile(ctx context.Context, p Processor) error {
	for {
		conn, err := l.connector.AcquireWrite(ctx)
		if err != nil {
			return fmt.Errorf("failed to acquire conn: %w", err)
		}

		err = l.awaitNotifications(ctx, conn, p)
		conn.Release()

		if errors.Is(err, context.Canceled) {
			return nil
		}

		if ctx.Err() != nil {
			return fmt.Errorf("listener: %s", ctx.Err())
		}
	}
}

func (l *Listener) awaitNotifications(ctx context.Context, conn *pgxpool.Conn, p Processor) error {
	for {
		// new sub-context for WaitForNotification
		waitCtx, cancel := context.WithCancel(ctx)

		l.mu.Lock()
		l.cancel = cancel
		if !l.started {
			l.started = true
			l.ready.Broadcast() // wake any ValueChanged waiting
		}
		l.mu.Unlock()

		notification, err := conn.Conn().WaitForNotification(waitCtx)

		if err != nil {
			// context canceled → means ValueChanged was called or global ctx canceled
			if waitCtx.Err() != nil && ctx.Err() == nil {
				// interrupted by ValueChanged, drain commands and continue loop
				if err := l.applyCommands(ctx, conn); err != nil {
					return err
				}
				continue
			}
			// real connection error or global cancel
			return err
		}

		if notification != nil {
			err := l.process(ctx, p, notification.Payload)
			if err != nil {
				return err
			}
		}
	}
}

func (l *Listener) ValuesChanged(_ context.Context, values, added, removed []uint32) error {
	l.mu.Lock()
	for !l.started { // block until first cancel is available
		l.ready.Wait()
	}
	cancel := l.cancel // capture under lock
	l.mu.Unlock()

	done := make(chan error)
	l.commands <- valueCommand{
		done:    done,
		added:   added,
		removed: removed,
	}

	// cancel current wait so commands can be applied
	if cancel != nil {
		cancel()
	}

	return <-done
}

// applyCommands executes LISTEN/UNLISTEN for queued commands.
func (l *Listener) applyCommands(ctx context.Context, conn *pgxpool.Conn) error {
	for {
		select {
		case cmd := <-l.commands:
			cmd.done <- errors.Join(
				l.schema.Listen(ctx, conn, cmd.added),
				l.schema.Unlisten(ctx, conn, cmd.removed),
			)
		default:
			// nothing left
			return nil
		}
	}
}

func (l *Listener) process(ctx context.Context, p Processor, payload string) error {
	stream, err := listenerpayload.Decode(payload)
	if err != nil {
		return err
	}
	processCtx, processCancel := context.WithTimeout(ctx, l.processTimeout)
	defer processCancel()

	return p.Process(processCtx, stream)
}

type valueCommand struct {
	done    chan error
	added   []uint32
	removed []uint32
}
