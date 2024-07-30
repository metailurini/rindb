package main

import (
	"crypto/rand"
	"errors"
	"math/big"
)

const (
	DefaultLevel = 2
	MaxLevel     = 32
	p            = 0.5
)

var ErrKeyNotFound = errors.New("key not found")

type node[K Comparable, V any] struct {
	key      K
	value    V
	forwards []*node[K, V]
}

func (n *node[K, V]) next() *node[K, V] {
	if n == nil {
		return nil
	}
	return n.forwards[0]
}

type skipList[K Comparable, V any] struct {
	level    uint
	length   uint
	headNote *node[K, V]
}

func initSkipList[K Comparable, V any]() (*skipList[K, V], error) {
	var emptyKeyValue K
	err := ValidateCmpType(emptyKeyValue)
	if err != nil {
		return nil, err
	}

	return &skipList[K, V]{
		level:    DefaultLevel,
		headNote: &node[K, V]{forwards: make([]*node[K, V], DefaultLevel)},
	}, nil
}

func (list *skipList[K, V]) put(searchKey K, newValue V) {
	rl := list.level
	rn := list.head()
	update := make([]*node[K, V], MaxLevel)
	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].key, searchKey) == -1 {
			rn = rn.forwards[rl]
		}
		update[rl] = rn
	}

	if rn.forwards[0] != nil {
		rn = rn.forwards[0]
	}
	if Compare(rn.key, searchKey) == 0 {
		rn.value = newValue
	} else {
		newLevel := randomLevel()
		if newLevel > list.level {
			rl := newLevel
			for rl > list.level {
				rl--
				update[rl] = list.head()
				update[rl].forwards = append(update[rl].forwards, make([]*node[K, V], newLevel-list.level)...)
			}
			list.level = newLevel
		}
		newNode := &node[K, V]{
			key:      searchKey,
			value:    newValue,
			forwards: make([]*node[K, V], list.level),
		}
		for newLevel > 0 {
			newLevel--
			newNode.forwards[newLevel] = update[newLevel].forwards[newLevel]
			update[newLevel].forwards[newLevel] = newNode
		}

		list.length++
	}
}

func (list *skipList[K, V]) get(searchKey K) (V, error) {
	rl := list.level
	rn := list.head()

	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].key, searchKey) == -1 {
			rn = rn.forwards[rl]
		}
	}
	if rn.forwards[0] != nil {
		rn = rn.forwards[0]
	}
	if Compare(rn.key, searchKey) == 0 {
		return rn.value, nil
	} else {
		var emptyValue V
		return emptyValue, ErrKeyNotFound
	}
}

func (list *skipList[K, V]) head() *node[K, V] {
	return list.headNote
}

func (list *skipList[K, V]) remove(searchKey K) error {
	rl := list.level
	rn := list.head()
	update := make([]*node[K, V], MaxLevel)
	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].key, searchKey) == -1 {
			rn = rn.forwards[rl]
		}
		update[rl] = rn
	}

	if rn.forwards[0] != nil {
		rn = rn.forwards[0]
	}
	if Compare(rn.key, searchKey) == 0 {
		for i := 0; i < int(list.level); i++ {
			if update[i].forwards[i] != rn {
				break
			}
			update[i].forwards[i] = rn.forwards[i]
		}
		for list.level > 1 && list.head().forwards[list.level-1] == nil {
			list.level--
		}
	} else {
		return ErrKeyNotFound
	}

	list.length--
	return nil
}

func (list skipList[K, V]) len() uint {
	return list.length
}

func intn(m int64) int64 {
	nBig, err := rand.Int(rand.Reader, big.NewInt(m))
	if err != nil {
		panic(err)
	}
	return nBig.Int64()
}

func randF64() float64 {
	const m = 53
	return float64(intn(1<<m)) / (1 << m)
}

func randomLevel() uint {
	lvl := uint(1)
	for lvl < MaxLevel {
		randFloat := randF64()
		if randFloat >= p {
			break
		}
		lvl++
	}
	return lvl
}
