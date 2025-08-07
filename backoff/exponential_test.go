package backoff_test

import (
	"testing"
	"time"

	"github.com/kyuff/es-postgres/backoff"
)

func TestExponential(t *testing.T) {
	tests := []struct {
		name    string
		base    time.Duration
		retries int64
		want    time.Duration
	}{
		{"zero retries", time.Second, 0, time.Second},
		{"one retry", time.Second, 1, 2 * time.Second},
		{"two retries", time.Second, 2, 4 * time.Second},
		{"three retries", time.Second, 3, 8 * time.Second},
		{"four retries", time.Second, 4, 16 * time.Second},
		{"five retries", time.Second, 5, 32 * time.Second},
		{"zero base", 0, 3, 0},
		{"large retries", time.Millisecond, 10, 1024 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := backoff.Exponential(tt.base, tt.retries)
			if got != tt.want {
				t.Errorf("Exponential(%v, %d) = %v, want %v", tt.base, tt.retries, got, tt.want)
			}
		})
	}
}
