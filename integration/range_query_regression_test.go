//go:build integration && smoke

package rindb_test

import (
	"fmt"
	"testing"
)

// TestIRangeRangeQueryDeterministic runs TestIRangeRangeQuery multiple times
// to ensure deterministic results across runs.
func TestIRangeRangeQueryDeterministic(t *testing.T) {
	for i := 0; i < 5; i++ {
		t.Run(fmt.Sprintf("run-%d", i), TestIRangeRangeQuery)
	}
}
