package rindb

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter(t *testing.T) {
	wordPresent := []string{
		"abound", "abounds", "abundance", "abundant", "accessible",
		"bloom", "blossom", "bolster", "bonny", "bonus", "bonuses",
		"coherent", "cohesive", "colorful", "comely", "comfort",
		"gems", "generosity", "generous", "generously", "genial",
	}
	wordAbsent := []string{
		"bluff", "cheater", "hate", "war", "humanity",
		"racism", "hurt", "nuke", "gloomy", "facebook",
		"geeksforgeeks", "twitter",
	}

	l := uint64(len(wordPresent))
	b := NewBloomFilter(
		SetN(l),
		SetP(10e-100),
		WithCalculatedM(),
		// WithCalculatedK(),
		SetK(4),
	)

	falsePositive := b.FalsePositive()
	t.Logf("probability of false positive: %f %%", falsePositive*100)

	for _, word := range wordPresent {
		b.Insert(Bytes(word))
	}

	for _, word := range wordAbsent {
		if !b.Lookup(Bytes(word)) {
			fmt.Printf("word: %v\n", word)
			assert.False(t, slices.Contains(wordPresent, word))
		}
	}

	for _, word := range wordPresent {
		assert.True(t, b.Lookup(Bytes(word)))
	}
}

func TestBloomFilterOpts(t *testing.T) {
	t.Run("Set all params manually", func(t *testing.T) {
		b := NewBloomFilter(
			SetN(100),
			SetP(10e-100),
			SetM(1000),
			SetK(4),
		)
		assert.Equal(t, uint64(100), b.config.n)
		assert.Equal(t, float64(10e-100), b.config.p)
		assert.Equal(t, uint32(1000), b.config.m)
		assert.Equal(t, uint32(4), b.config.k)
	})

	t.Run("With calculated m", func(t *testing.T) {
		b := NewBloomFilter(
			SetN(100),
			SetP(10e-100),
			WithCalculatedM(),
			SetK(4),
		)
		assert.Equal(t, uint64(100), b.config.n)
		assert.Equal(t, float64(10e-100), b.config.p)
		assert.False(t, isEmpty(b.config.m))
		assert.LessOrEqual(t, b.FalsePositive(), .1, "False positive rate too hight")
	})

	t.Run("With calculated k", func(t *testing.T) {
		b := NewBloomFilter(
			SetN(100),
			SetP(10e-100),
			SetM(1000),
			WithCalculatedK(),
		)
		assert.Equal(t, uint64(100), b.config.n)
		assert.Equal(t, float64(10e-100), b.config.p)
		assert.Equal(t, uint32(1000), b.config.m)
		assert.False(t, isEmpty(b.config.k))
		assert.LessOrEqual(t, b.FalsePositive(), .1, "False positive rate too hight")
	})
}
