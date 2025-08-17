package retry

import (
	"context"
	"fmt"
	"time"
)

func Continue(ctx context.Context, interval time.Duration, maxErrors int, fn func(ctx context.Context) error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var errorCount = 0
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return nil
		case <-ticker.C:
			err := fn(ctx)
			if err != nil {
				errorCount++
			} else {
				errorCount = 0
			}

			if errorCount > maxErrors {
				return fmt.Errorf("max errors reached")
			}
		}
	}
}
