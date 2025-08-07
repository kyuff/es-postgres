package backoff

import "time"

func Exponential(base time.Duration, retries int64) time.Duration {
	return base * time.Duration(1<<retries)
}
