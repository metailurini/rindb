package rindb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockCmpType struct {
	value int
}

func (m mockCmpType) Compare(other any) int {
	o := other.(mockCmpType)
	if m.value < o.value {
		return -1
	} else if m.value > o.value {
		return 1
	}
	return 0
}

//nolint:gocognit,funlen,gocyclo
func TestComparable(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		tests := []struct {
			a, b     int
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("int8", func(t *testing.T) {
		tests := []struct {
			a, b     int8
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("int16", func(t *testing.T) {
		tests := []struct {
			a, b     int16
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("int32", func(t *testing.T) {
		tests := []struct {
			a, b     int32
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("int64", func(t *testing.T) {
		tests := []struct {
			a, b     int64
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("uint", func(t *testing.T) {
		tests := []struct {
			a, b     uint
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("uint8", func(t *testing.T) {
		tests := []struct {
			a, b     uint8
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("uint16", func(t *testing.T) {
		tests := []struct {
			a, b     uint16
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("uint32", func(t *testing.T) {
		tests := []struct {
			a, b     uint32
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("uint64", func(t *testing.T) {
		tests := []struct {
			a, b     uint64
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("uintptr", func(t *testing.T) {
		tests := []struct {
			a, b     uintptr
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d vs %d", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("float32", func(t *testing.T) {
		tests := []struct {
			a, b     float32
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%f vs %f", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%f, %f) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("float64", func(t *testing.T) {
		tests := []struct {
			a, b     float64
			expected int
		}{
			{1, 2, -1},
			{2, 1, 1},
			{1, 1, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%f vs %f", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%f, %f) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("string", func(t *testing.T) {
		tests := []struct {
			a, b     string
			expected int
		}{
			{"1", "2", -1},
			{"2", "1", 1},
			{"1", "1", 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s vs %s", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%s, %s) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("MyCmpType", func(t *testing.T) {
		tests := []struct {
			a, b     mockCmpType
			expected int
		}{
			{mockCmpType{1}, mockCmpType{2}, -1},
			{mockCmpType{2}, mockCmpType{1}, 1},
			{mockCmpType{1}, mockCmpType{1}, 0},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%v vs %v", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.NoError(t, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%v, %v) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})

	t.Run("Unsupported Type", func(t *testing.T) {
		tests := []struct {
			a, b     struct{ int }
			expected int
		}{
			{struct{ int }{1}, struct{ int }{1}, UnsupportedTypeCode},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%v vs %v", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.ErrorIs(t, ErrUnsupportedType, err)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%v, %v) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})
}
