package main

type bitset []uint64

func NewBitset(size uint) bitset {
	return make(bitset, (size+64)/64)
}

func (b bitset) Set(i int) {
	b[i/64] |= 1 << (i % 64)
}

func (b bitset) Get(i int) uint {
	return uint(b[i/64] & (1 << (i % 64)))
}
