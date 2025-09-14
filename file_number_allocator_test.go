package rindb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileNumberAllocator_Apply(t *testing.T) {
	tests := []struct {
		name     string
		start    uint64
		edit     versionEdit
		expected uint64
	}{
		{
			name:     "no update",
			start:    3,
			edit:     versionEdit{},
			expected: 3,
		},
		{
			name:     "update",
			start:    5,
			edit:     versionEdit{NextFileNumber: 9},
			expected: 9,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			a := newFileNumberAllocator(tt.start)
			a.apply(tt.edit)
			require.Equal(t, tt.expected, a.peek())
		})
	}
}
