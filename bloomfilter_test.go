package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter_BasicOperations(t *testing.T) {
	t.Parallel()
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
		t.Run(tt.name, func(t *testing.T) {
			for _, w := range tt.words {
				assert.Equal(t, tt.want, b.Lookup(Bytes(w)))
			}
		})
	}
}

func TestBloomFilter_Options(t *testing.T) {
	t.Parallel()
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

func BenchmarkBloomFilter_Insert(b *testing.B) {
	bloom := NewBloomFilter(
		SetN(1<<12),
		SetP(0.01),
		WithCalculatedM(),
		SetK(6),
	)
	key := Bytes("benchmark-key")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bloom.Insert(key)
	}
}

func BenchmarkBloomFilter_Lookup(b *testing.B) {
	bloom := NewBloomFilter(
		SetN(1<<12),
		SetP(0.01),
		WithCalculatedM(),
		SetK(6),
	)
	key := Bytes("benchmark-key")
	bloom.Insert(key)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !bloom.Lookup(key) {
			b.Fatalf("expected key to be present")
		}
	}
}

func TestBloomFilter_InsertSkipsWhenDisabled(t *testing.T) {
	t.Parallel()

	bloom := NewBloomFilter(
		SetN(1),
		SetP(0.5),
		SetM(8),
		SetK(0),
	)

	before := append([]uint64(nil), bloom.bucket.set...)

	bloom.Insert(Bytes("any"))

	assert.Equal(t, before, bloom.bucket.set)
}

func TestBloomFilter_InsertSkipsWhenBucketEmpty(t *testing.T) {
	t.Parallel()

	bloom := NewBloomFilter(
		SetN(1),
		SetP(0.5),
		SetM(0),
		SetK(3),
	)

	before := append([]uint64(nil), bloom.bucket.set...)

	bloom.Insert(Bytes("any"))

	assert.Equal(t, before, bloom.bucket.set)
}

func TestBloomFilter_LookupDisabledConfigurations(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		filter *BloomFilter
	}{
		{
			name: "no hash functions",
			filter: NewBloomFilter(
				SetN(1),
				SetP(0.5),
				SetM(8),
				SetK(0),
			),
		},
		{
			name: "empty bucket",
			filter: NewBloomFilter(
				SetN(1),
				SetP(0.5),
				SetM(0),
				SetK(3),
			),
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, tt.filter.Lookup(Bytes("any")))
		})
	}
}

func TestEnsureOdd(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   uint64
		want uint64
	}{
		{name: "already odd", in: 5, want: 5},
		{name: "even becomes odd", in: 8, want: 9},
		{name: "zero becomes one", in: 0, want: 1},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ensureOdd(tt.in))
		})
	}
}
