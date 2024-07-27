package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter_Insert(t *testing.T) {
	wordPresent := []string{
		"abound", "abounds", "abundance", "abundant", "accessible",
		"bloom", "blossom", "bolster", "bonny", "bonus", "bonuses",
		"coherent", "cohesive", "colorful", "comely", "comfort",
		"gems", "generosity", "generous", "generously", "genial",
	}
	wordAbsent := []string{
		"bluff",
		"cheater", "hate", "war", "humanity",
		"racism", "hurt", "nuke", "gloomy", "facebook",
		"geeksforgeeks", "twitter",
	}

	l := uint(len(wordPresent))
	fmt.Printf("l: %v\n", l)
	b := NewBloomFilter(l, 10e-200)

	falsePositive := b.FalsePositive(l)
	t.Logf("probability of false positive: %f %%", falsePositive*100)

	for _, word := range wordPresent {
		b.Insert(word)
	}

	for _, word := range wordAbsent {
		assert.False(t, b.Lookup(word))
	}
}
