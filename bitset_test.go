package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitset_Set(t *testing.T) {
	m := uint(1000000)
	b := NewBitset(m)

	testSkip := func(t *testing.T, skipNum uint) {
		for i := m; i > 0; i-- {
			if i%skipNum == 0 {
				b.Set(int(i))
			}
		}

		for i := m; i > 0; i-- {
			if i%skipNum == 0 {
				assert.Greater(t, b.Get(int(i)), uint(0))
			}
		}
	}

	for i := uint(2); i < 9; i++ {
		t.Run(fmt.Sprintf("Skip with mod %d", i), func(t *testing.T) {
			testSkip(t, i)
		})
	}
}

func BenchmarkBitset_Set(b *testing.B) {
	bs := NewBitset(uint(b.N))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bs.Set(i)
	}
}

func BenchmarkBitset_Get(b *testing.B) {
	bs := NewBitset(uint(b.N))
	for i := 0; i < b.N; i++ {
		bs.Set(i)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bs.Get(i)
	}
}
