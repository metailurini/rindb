package rindb

import (
	"crypto/rand"
	"errors"
	"math/big"
)

var (
	ErrMalformedList = errors.New("the list was not init-ed properly")
)

type SLNode[K Comparable, V any] struct {
	Key      K
	Value    V
	forwards []*SLNode[K, V]
}

func (n *SLNode[K, V]) Next() *SLNode[K, V] {
	return n.forwards[0]
}

type SkipList[K Comparable, V any] struct {
	level    uint
	length   uint
	headNote *SLNode[K, V]
	config   Config
}

func InitSkipList[K Comparable, V any](config Config) (*SkipList[K, V], error) {
	var emptyKeyValue K
	err := ValidateCmpType(emptyKeyValue)
	if err != nil {
		return nil, err
	}

	return &SkipList[K, V]{
		level:    config.skipListDefaultLevel,
		headNote: &SLNode[K, V]{forwards: make([]*SLNode[K, V], config.skipListDefaultLevel)},
		config:   config,
	}, nil
}

func (list *SkipList[K, V]) Put(searchKey K, newValue V) {
	rn := list.Head()
	rl := list.level
	update := make([]*SLNode[K, V], list.config.skipListMaxLevel)
	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].Key, searchKey) == CmpLess {
			rn = rn.forwards[rl]
		}
		update[rl] = rn
	}

	if rn.forwards[0] != nil {
		rn = rn.forwards[0]
	}
	if Compare(rn.Key, searchKey) == CmpEqual {
		rn.Value = newValue
	} else {
		newLevel := randomLevel(list.config)
		if newLevel > list.level {
			rl := newLevel
			for rl > list.level {
				rl--
				update[rl] = list.Head()
				update[rl].forwards = append(update[rl].forwards, make([]*SLNode[K, V], newLevel-list.level)...)
			}
			list.level = newLevel
		}
		newNode := &SLNode[K, V]{
			Key:      searchKey,
			Value:    newValue,
			forwards: make([]*SLNode[K, V], list.level),
		}
		for newLevel > 0 {
			newLevel--
			newNode.forwards[newLevel] = update[newLevel].forwards[newLevel]
			update[newLevel].forwards[newLevel] = newNode
		}

		list.length++
	}
}

func (list *SkipList[K, V]) Get(searchKey K) (V, error) {
	rn := list.Head()
	rl := list.level

	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].Key, searchKey) == CmpLess {
			rn = rn.forwards[rl]
		}
	}
	if rn.forwards[0] != nil {
		rn = rn.forwards[0]
	}
	if Compare(rn.Key, searchKey) == CmpEqual {
		return rn.Value, nil
	} else {
		var emptyValue V
		return emptyValue, ErrKeyNotFound
	}
}

func (list *SkipList[K, V]) Head() *SLNode[K, V] {
	if list == nil || list.headNote == nil {
		panic(ErrMalformedList)
	}

	return list.headNote
}

func (list *SkipList[K, V]) Remove(searchKey K) error {
	rn := list.Head()
	rl := list.level
	update := make([]*SLNode[K, V], list.config.skipListMaxLevel)
	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].Key, searchKey) == CmpLess {
			rn = rn.forwards[rl]
		}
		update[rl] = rn
	}

	if rn.forwards[0] != nil {
		rn = rn.forwards[0]
	}
	if Compare(rn.Key, searchKey) == CmpEqual {
		for i := 0; i < int(list.level); i++ {
			if update[i].forwards[i] != rn {
				break
			}
			update[i].forwards[i] = rn.forwards[i]
		}
		for list.level > 1 && list.Head().forwards[list.level-1] == nil {
			list.level--
		}
	} else {
		return ErrKeyNotFound
	}

	list.length--
	return nil
}

func (list *SkipList[K, V]) Clear() {
	if list == nil {
		panic(ErrMalformedList)
	}
	newList, err := InitSkipList[K, V](list.config)
	if err != nil {
		panic(ErrMalformedList)
	}

	list.level = newList.level
	list.length = newList.length
	list.headNote = newList.headNote
}

func (list *SkipList[K, V]) Len() uint {
	if list == nil {
		panic(ErrMalformedList)
	}
	return list.length
}

var _ Iterator[any] = (*slIterator[Comparable, any])(nil)

type slIterator[K Comparable, V any] struct {
	node *SLNode[K, V]
}

// HasNext implements Iterator.
func (s *slIterator[K, V]) HasNext() bool {
	return s.node != nil
}

// Next implements Iterator.
func (s *slIterator[K, V]) Next() (V, error) {
	if !s.HasNext() {
		var empty V
		return empty, EOI
	}
	value := s.node.Value
	s.node = s.node.Next()
	return value, nil
}

func (list *SkipList[K, V]) Iterator() Iterator[V] {
	return &slIterator[K, V]{
		node: list.Head().Next(),
	}
}

// slIRange iterates over a key range within the skip list.
type slIRange[K Comparable, V any] struct {
	node   *SLNode[K, V]
	endKey K
}

// HasNext implements Iterator.
func (s *slIRange[K, V]) HasNext() bool {
	return s.node != nil && Compare(s.node.Key, s.endKey) != CmpGreater
}

// Next implements Iterator.
func (s *slIRange[K, V]) Next() (V, error) {
	if !s.HasNext() {
		var empty V
		return empty, EOI
	}
	value := s.node.Value
	s.node = s.node.Next()
	return value, nil
}

// IRange returns an iterator over records with keys in [start, end].
func (list *SkipList[K, V]) IRange(start, end K) Iterator[V] {
	rn := list.Head()
	rl := list.level
	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].Key, start) == CmpLess {
			rn = rn.forwards[rl]
		}
	}
	rn = rn.forwards[0]
	return &slIRange[K, V]{
		node:   rn,
		endKey: end,
	}
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

func randomLevel(config Config) uint {
	lvl := uint(1)
	for lvl < config.skipListMaxLevel {
		randFloat := randF64()
		if randFloat >= config.skipListP {
			break
		}
		lvl++
	}
	return lvl
}
