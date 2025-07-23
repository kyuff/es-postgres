package seqs_test

import (
	"testing"

	"github.com/kyuff/es-postgres/internal/assert"
	"github.com/kyuff/es-postgres/internal/seqs"
)

func TestEmptySeq2(t *testing.T) {
	// act
	got := seqs.EmptySeq2[string, error]()

	// assert
	assert.NotNil(t, got)
	for range got {
		t.Error("should be empty")
	}
}
