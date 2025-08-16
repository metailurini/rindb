package rindb

import (
	"math"

	"github.com/spaolacci/murmur3"
)

// bloomFilterConfig represents the configuration parameters for a Bloom filter.
type bloomFilterConfig struct {
	// m is the number of bits in the Bloom filter.
	m uint32

	// n is the number of elements inserted into the Bloom filter.
	n uint64

	// p is the desired probability of a false positive.
	p float64

	// k is the optimal number of hash functions to use.
	k uint32
}

// BloomFilterOpt is a functional option type for configuring a Bloom filter.
type BloomFilterOpt func(cfg *bloomFilterConfig)

// SetN sets the number of inserted elements for the Bloom filter.
func SetN(n uint64) BloomFilterOpt {
	return func(cfg *bloomFilterConfig) {
		cfg.n = n
	}
}

// SetP sets the probability of a false positive for the Bloom filter.
func SetP(p float64) BloomFilterOpt {
	return func(cfg *bloomFilterConfig) {
		cfg.p = p
	}
}

// SetM sets the number of bits for the Bloom filter.
func SetM(m uint32) BloomFilterOpt {
	return func(cfg *bloomFilterConfig) {
		cfg.m = m
	}
}

// SetK sets the optimal number of hash functions for the Bloom filter.
func SetK(k uint32) BloomFilterOpt {
	return func(cfg *bloomFilterConfig) {
		cfg.k = k
	}
}

// WithCalculatedM sets the number of bits for the Bloom filter.
// To use this option, n and p must be set first
func WithCalculatedM() BloomFilterOpt {
	return func(cfg *bloomFilterConfig) {
		if isEmpty(cfg.n) {
			panic("Number of inserted elements (n) cannot be empty")
		}
		if isEmpty(cfg.p) {
			panic("Probability of false positive (p) cannot be empty")
		}

		const squaredPower = 2
		m := -1 * (float64(cfg.n) * math.Log(cfg.p)) / (math.Pow(math.Ln2, squaredPower))
		cfg.m = uint32(m)
	}
}

// WithCalculatedK sets the optimal number of hash functions for the Bloom filter.
// To use this option, m and n must be set first
func WithCalculatedK() BloomFilterOpt {
	return func(cfg *bloomFilterConfig) {
		if isEmpty(cfg.m) {
			panic("Number of bits (m) cannot be empty")
		}
		if isEmpty(cfg.n) {
			panic("Number of inserted elements (n) cannot be empty")
		}

		k := float64(cfg.m) / float64(cfg.n) * math.Ln2
		cfg.k = uint32(k)
	}
}

// BloomFilter represents a probabilistic data structure used for efficient membership testing.
type BloomFilter struct {
	// Configuration settings for the Bloom filter
	config bloomFilterConfig

	// Bitset to store the presence of elements
	bucket Bitset
}

// NewBloomFilter creates a new BloomFilter with the specified options.
// n, p, m and k are mandatory params.
func NewBloomFilter(options ...BloomFilterOpt) *BloomFilter {
	cfg := &bloomFilterConfig{}
	for _, optionFn := range options {
		optionFn(cfg)
	}
	bucket := NewBitset(cfg.m)
	return &BloomFilter{
		config: *cfg,
		bucket: bucket,
	}
}

// Insert adds a string to the BloomFilter.
func (b *BloomFilter) Insert(str Bytes) {
	l := b.bucket.size
	for i := b.config.k; i > 0; i-- {
		hv := hashStr(str, i)
		b.bucket.Set(hv % l)
	}
}

// Lookup checks if a given string is likely to be in the Bloom filter.
func (b *BloomFilter) Lookup(str Bytes) bool {
	l := b.bucket.size
	for i := b.config.k; i > 0; i-- {
		hv := hashStr(str, i)
		if !b.bucket.Test(hv % l) {
			return false
		}
	}
	return true
}

// FalsePositive calculates the probability of a false positive in the current Bloom filter.
func (b *BloomFilter) FalsePositive() float64 {
	m := float64(b.bucket.size)
	k := float64(b.config.k)
	n := float64(b.config.n)
	P := math.Pow(1-math.Pow(1-1/m, k*n), k)
	return P
}

// hashStr calculates the hash value of the given string using the Murmur3 algorithm.
func hashStr(str Bytes, seed uint32) uint32 {
	h := murmur3.New32WithSeed(seed)
	_, _ = h.Write(str)
	return h.Sum32()
}
