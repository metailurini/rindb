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

func TestBitset_Init(t *testing.T) {
}

func TestBitset_Set(t *testing.T) {
	t.Run("Set index bigger than size", func(t *testing.T) {
		bitset := NewBitset(0)
		bitset.Set(0)
		assert.False(t, bitset.Test(9999))
		assert.Equal(t, uint32(0), bitset.size)
	})

	t.Run("huge size", func(t *testing.T) {
		size := uint32(1000)

		for skipNum := uint32(2); skipNum < 9; skipNum++ {
			bitset := NewBitset(size)
			assert.Equal(t, size, bitset.size)

			for i := size - 1; i > 0; i-- {
				if i%skipNum == 0 {
					bitset.Set(i)
				}
			}

			for i := size - 1; i > 0; i-- {
				if i%skipNum == 0 {
					assert.True(t, bitset.Test(i))
				} else {
					assert.False(t, bitset.Test(i))
				}
			}
		}
	})
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
