package main

import (
	"math"

	"github.com/spaolacci/murmur3"
)

var HashQuantity uint32 = 4

type BloomFilter struct {
	hashQuantity uint32
	bucket       bitset
}

func EstimateBloomFilterParams(numberElements uint, falsePositiveProbability float64) (bucketSize uint, numHashFunctions uint32) {
	n := float64(numberElements)
	P := float64(falsePositiveProbability)

	m := n * math.Log(P) / (math.Log(0.5) * math.Log(2))
	k := HashQuantity

	bucketSize = uint(m)
	numHashFunctions = uint32(k)
	return
}

func NewBloomFilter(numberElements uint, falsePositiveProbability float64) *BloomFilter {
	bucketSize, numHashFunctions := EstimateBloomFilterParams(numberElements, falsePositiveProbability)
	b := new(BloomFilter)
	b.bucket = NewBitset(bucketSize)
	b.hashQuantity = numHashFunctions
	return b
}

func (b *BloomFilter) Insert(str string) {
	l := len(b.bucket)
	for i := b.hashQuantity; i > 0; i-- {
		hv := hashStr(str, i)
		b.bucket.Set(int(hv) % l)
	}
}

func (b *BloomFilter) Lookup(str string) bool {
	l := len(b.bucket)
	for i := b.hashQuantity; i > 0; i-- {
		hv := hashStr(str, i)
		if b.bucket.Get(int(hv)%l) == 0 {
			return false
		}
	}
	return true
}

func (b *BloomFilter) FalsePositive(numberElements uint) float64 {
	m := float64(len(b.bucket))
	k := float64(HashQuantity)
	n := float64(numberElements)

	return math.Pow(1-math.Pow(1-1/m, k*n), k)
}

func hashStr(str string, seed uint32) uint32 {
	h := murmur3.New32WithSeed(seed)
	h.Write([]byte(str))
	return h.Sum32()
}
