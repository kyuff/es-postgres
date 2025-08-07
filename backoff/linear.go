package backoff

import "time"

func Linear(increment time.Duration, retries int64) time.Duration {
	return increment * time.Duration(retries)
}
