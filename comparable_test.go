package rindb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type customCmpType struct {
	value int
}

func (c customCmpType) Compare(other any) int {
	o := other.(customCmpType)
	if c.value < o.value {
		return -1
	} else if c.value > o.value {
		return 1
	}
	return 0
}

//nolint:gocognit,funlen,gocyclo
func TestComparable(t *testing.T) {
	t.Run("Int", func(t *testing.T) {
		tests := []struct {
			a, b     int
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Int8", func(t *testing.T) {
		tests := []struct {
			a, b     int8
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Int16", func(t *testing.T) {
		tests := []struct {
			a, b     int16
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Int32", func(t *testing.T) {
		tests := []struct {
			a, b     int32
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Int64", func(t *testing.T) {
		tests := []struct {
			a, b     int64
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Uint", func(t *testing.T) {
		tests := []struct {
			a, b     uint
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Uint8", func(t *testing.T) {
		tests := []struct {
			a, b     uint8
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Uint16", func(t *testing.T) {
		tests := []struct {
			a, b     uint16
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Uint32", func(t *testing.T) {
		tests := []struct {
			a, b     uint32
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Uint64", func(t *testing.T) {
		tests := []struct {
			a, b     uint64
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Uintptr", func(t *testing.T) {
		tests := []struct {
			a, b     uintptr
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Float32", func(t *testing.T) {
		tests := []struct {
			a, b     float32
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("Float64", func(t *testing.T) {
		tests := []struct {
			a, b     float64
			expected CompareResult
		}{
			{1, 2, CmpLess},
			{2, 1, CmpGreater},
			{1, 1, CmpEqual},
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

	t.Run("String", func(t *testing.T) {
		tests := []struct {
			a, b     string
			expected CompareResult
		}{
			{"1", "2", CmpLess},
			{"2", "1", CmpGreater},
			{"1", "1", CmpEqual},
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
			a, b     customCmpType
			expected CompareResult
		}{
			{customCmpType{1}, customCmpType{2}, CmpLess},
			{customCmpType{2}, customCmpType{1}, CmpGreater},
			{customCmpType{1}, customCmpType{1}, CmpEqual},
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
			expected CompareResult
		}{
			{struct{ int }{1}, struct{ int }{1}, UnsupportedTypeCode},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%v vs %v", tt.a, tt.b), func(t *testing.T) {
				err := ValidateCmpType(tt.a)
				assert.ErrorIs(t, err, ErrUnsupportedType)
				result := Compare(tt.a, tt.b)
				if result != tt.expected {
					t.Errorf("Compare(%v, %v) = %d; want %d", tt.a, tt.b, result, tt.expected)
				}
			})
		}
	})
}
