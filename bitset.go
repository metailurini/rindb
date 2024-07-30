package main

const (
	bitSize = 64
)

type bitset []uint64

func NewBitset(size uint) bitset {
	return make(bitset, (size+bitSize)/bitSize)
}

func (b bitset) Set(i int) {
	b[i/bitSize] |= 1 << (i % bitSize)
}

func (b bitset) Get(i int) uint {
	return uint(b[i/bitSize] & (1 << (i % bitSize)))
}
