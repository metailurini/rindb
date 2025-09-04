package rindb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileNumberAllocatorApply(t *testing.T) {
	tests := []struct {
		name     string
		start    uint64
		edit     VersionEdit
		expected uint64
	}{
		{
			name:     "no update",
			start:    3,
			edit:     VersionEdit{},
			expected: 3,
		},
		{
			name:     "update",
			start:    5,
			edit:     VersionEdit{NextFileNumber: 9},
			expected: 9,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := NewFileNumberAllocator(tc.start)
			a.Apply(tc.edit)
			require.Equal(t, tc.expected, a.Peek())
		})
	}
}
