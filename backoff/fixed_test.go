package backoff_test

import (
	"testing"
	"time"

	"github.com/kyuff/es-postgres/backoff"
)

func TestFixed(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want time.Duration
	}{
		{"zero duration", 0, 0},
		{"one second", time.Second, time.Second},
		{"negative duration", -time.Second, -time.Second},
		{"large duration", 24 * time.Hour, 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := backoff.Fixed(tt.d)
			if got != tt.want {
				t.Errorf("Fixed(%v) = %v, want %v", tt.d, got, tt.want)
			}
		})
	}
}
