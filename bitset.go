package main

import "log"

const (
	bitSize = 64
)

type Bitset struct {
	set  []uint64
	size uint32
}

// NewBitset creates and returns a new bitset with enough capacity to hold size bits.
func NewBitset(size uint32) Bitset {
	d := make([]uint64, (size+bitSize)/bitSize)
	return Bitset{set: d, size: size}
}

// Set sets the bit at the specified index to 1.
func (b Bitset) Set(index uint32) {
	word, bit := index/bitSize, index%bitSize
	b.set[word] |= 1 << bit
}

// Test checks whether the bit at the specified index is set or not.
func (b Bitset) Test(index uint32) bool {
	word, bit := index/bitSize, index%bitSize
	if !(index < b.size) {
		log.Printf("Index %d out of bounds for Bitset of size %d", index, b.size)
		return false
	}
	return (b.set[word] & (1 << bit)) != 0
}

// TODO: implement extend method
