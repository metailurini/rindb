package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
TODO:
- input with various format

ref:
- https://github.com/bits-and-blooms/bitset/blob/67644e686bb4b1240a5032822ceaa4cbb7ff8d85/bitset_test.go
*/

func TestBitset_New(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		size uint32
	}{
		{name: "zero size", size: 0},
		{name: "non-zero size", size: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bs := NewBitset(tt.size)
			assert.Equal(t, tt.size, bs.size)
			for i := uint32(0); i < tt.size; i++ {
				assert.False(t, bs.Test(i))
			}
		})
	}
}

func TestBitset_Set(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		size   uint32
		action func(t *testing.T, size uint32)
	}{
		{
			name: "index bigger than size",
			size: 0,
			action: func(t *testing.T, size uint32) {
				bs := NewBitset(size)
				bs.Set(0)
				assert.False(t, bs.Test(9999))
				assert.Equal(t, uint32(0), bs.size)
			},
		},
		{
			name: "huge size",
			size: 1000,
			action: func(t *testing.T, size uint32) {
				for skipNum := uint32(2); skipNum < 9; skipNum++ {
					bs := NewBitset(size)
					for i := size - 1; i > 0; i-- {
						if i%skipNum == 0 {
							bs.Set(i)
						}
					}
					for i := size - 1; i > 0; i-- {
						if i%skipNum == 0 {
							assert.True(t, bs.Test(i))
						} else {
							assert.False(t, bs.Test(i))
						}
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.action(t, tt.size)
		})
	}
}

func BenchmarkBitset_Test(b *testing.B) {
	n := uint32(b.N)
	bs := NewBitset(n)
	b.ResetTimer()

	for i := n; i > 0; i-- {
		bs.Set(i)
	}
}

func BenchmarkBitset_Get(b *testing.B) {
	n := uint32(b.N)
	bs := NewBitset(n)
	for i := n; i > 0; i-- {
		bs.Set(i)
	}

	b.ResetTimer()
	for i := n; i > 0; i-- {
		bs.Test(i)
	}
}
