package backoff

import "time"

func Fixed(d time.Duration) time.Duration {
	return d
}
