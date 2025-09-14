//go:build integration && smoke

package rindb_test

import (
	"fmt"
	"testing"
)

// TestIRange_ReturnsOrderedKeysDeterministic runs TestIRange_ReturnsOrderedKeys
// multiple times to ensure deterministic results across runs.
func TestIRange_ReturnsOrderedKeysDeterministic(t *testing.T) {
	for i := 0; i < 5; i++ {
		t.Run(fmt.Sprintf("run-%d", i), TestIRange_ReturnsOrderedKeys)
	}
}
