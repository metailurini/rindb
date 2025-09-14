package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter_BasicOperations(t *testing.T) {
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
		SetK(4),
	)

	falsePositive := b.FalsePositive()
	t.Logf("probability of false positive: %f %%", falsePositive*100)

	for _, word := range wordPresent {
		b.Insert(Bytes(word))
	}

	cases := []struct {
		name  string
		words []string
		want  bool
	}{
		{"lookup present words", wordPresent, true},
		{"lookup absent words", wordAbsent, false},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for _, w := range tt.words {
				assert.Equal(t, tt.want, b.Lookup(Bytes(w)))
			}
		})
	}
}

func TestBloomFilter_Options(t *testing.T) {
	cases := []struct {
		name    string
		opts    []BloomFilterOpt
		wantN   uint64
		wantP   float64
		wantM   uint32
		wantK   uint32
		checkM  bool
		checkK  bool
		checkFP bool
	}{
		{
			name:  "set all params manually",
			opts:  []BloomFilterOpt{SetN(100), SetP(10e-100), SetM(1000), SetK(4)},
			wantN: 100,
			wantP: 10e-100,
			wantM: 1000,
			wantK: 4,
		},
		{
			name:    "with calculated m",
			opts:    []BloomFilterOpt{SetN(100), SetP(10e-100), WithCalculatedM(), SetK(4)},
			wantN:   100,
			wantP:   10e-100,
			wantK:   4,
			checkM:  true,
			checkFP: true,
		},
		{
			name:    "with calculated k",
			opts:    []BloomFilterOpt{SetN(100), SetP(10e-100), SetM(1000), WithCalculatedK()},
			wantN:   100,
			wantP:   10e-100,
			wantM:   1000,
			checkK:  true,
			checkFP: true,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			b := NewBloomFilter(tt.opts...)

			assert.Equal(t, tt.wantN, b.config.n)
			assert.Equal(t, tt.wantP, b.config.p)
			if tt.checkM {
				assert.False(t, isEmpty(b.config.m))
			} else {
				assert.Equal(t, tt.wantM, b.config.m)
			}
			if tt.checkK {
				assert.False(t, isEmpty(b.config.k))
			} else {
				assert.Equal(t, tt.wantK, b.config.k)
			}
			if tt.checkFP {
                                assert.LessOrEqual(t, b.FalsePositive(), .1, "False positive rate too high")
			}
		})
	}
}
