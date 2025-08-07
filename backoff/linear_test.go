package backoff_test

import (
	"testing"
	"time"

	"github.com/kyuff/es-postgres/backoff"
)

func TestLinear(t *testing.T) {
	tests := []struct {
		name      string
		increment time.Duration
		retries   int64
		want      time.Duration
	}{
		{"zero retries", time.Second, 0, 0},
		{"one retry", time.Second, 1, time.Second},
		{"two retries", time.Second, 2, 2 * time.Second},
		{"five retries", time.Second, 5, 5 * time.Second},
		{"zero increment", 0, 3, 0},
		{"negative increment", -time.Second, 2, -2 * time.Second},
		{"large increment", 24 * time.Hour, 2, 48 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := backoff.Linear(tt.increment, tt.retries)
			if got != tt.want {
				t.Errorf("Linear(%v, %d) = %v, want %v", tt.increment, tt.retries, got, tt.want)
			}
		})
	}
}
