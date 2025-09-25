package rindb

import (
	"crypto/rand"
	"errors"
	"math/big"
)

var (
	// ErrMalformedList is returned when a SkipList has not been initialized
	// properly. It is exported so callers interacting with SkipList can
	// detect improper initialization.
	ErrMalformedList = errors.New("the list was not init-ed properly")
)

// SLNode represents a single node within a SkipList. It is exported to allow
// advanced users to inspect or traverse the list directly.
type SLNode[K Comparable, V any] struct {
	Key      K
	Value    V
	forwards []*SLNode[K, V]
	backward *SLNode[K, V]
}

// Next returns the node's immediate successor on the lowest level.
func (n *SLNode[K, V]) Next() *SLNode[K, V] {
	return n.forwards[0]
}

// SkipList is a generic ordered map implemented with a probabilistic
// skip list. It is exported for users who need a stand‑alone sorted
// in-memory index structure.
type SkipList[K Comparable, V any] struct {
	level    uint
	length   uint
	headNote *SLNode[K, V]
	tail     *SLNode[K, V]
	config   Config
}

// InitSkipList creates a new empty SkipList using the provided configuration.
// The key type must satisfy Comparable; otherwise ErrUnsupportedType is
// returned.
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

// Put inserts or replaces the value associated with searchKey.
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

		pred := update[0]
		succ := newNode.forwards[0]
		newNode.backward = pred
		if succ != nil {
			succ.backward = newNode
		} else {
			list.tail = newNode
		}

		list.length++
	}
}

// Get retrieves the value associated with searchKey. If the key does not exist
// ErrKeyNotFound is returned.
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

// FindGreaterOrEqual returns the first node with key >= searchKey.
func (list *SkipList[K, V]) FindGreaterOrEqual(searchKey K) (*SLNode[K, V], error) {
	rn := list.Head()
	rl := list.level
	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].Key, searchKey) == CmpLess {
			rn = rn.forwards[rl]
		}
	}
	rn = rn.forwards[0]
	if rn == nil {
		return nil, ErrKeyNotFound
	}
	return rn, nil
}

func (list *SkipList[K, V]) findLessOrEqual(searchKey K) (*SLNode[K, V], bool) {
	rn := list.Head()
	rl := list.level
	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].Key, searchKey) != CmpGreater {
			rn = rn.forwards[rl]
		}
	}
	if rn == list.Head() {
		return nil, false
	}
	return rn, true
}

// Head returns the head sentinel node of the list.
func (list *SkipList[K, V]) Head() *SLNode[K, V] {
	if list == nil || list.headNote == nil {
		panic(ErrMalformedList)
	}

	return list.headNote
}

// Remove deletes the node with the given key. It returns ErrKeyNotFound if the
// key is absent.
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
		succ := rn.forwards[0]
		pred := rn.backward
		if succ != nil {
			succ.backward = pred
		}
		rn.backward = nil
		if list.tail == rn {
			if pred != nil && pred != list.Head() {
				list.tail = pred
			} else {
				list.tail = nil
			}
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

// Clear removes all entries from the list, resetting it to its initial state.
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
	list.tail = nil
}

// Len returns the number of elements currently stored in the list.
func (list *SkipList[K, V]) Len() uint {
	if list == nil {
		panic(ErrMalformedList)
	}
	return list.length
}

var _ Iterator[any] = (*slIterator[Comparable, any])(nil)

type slIterator[K Comparable, V any] struct {
	head *SLNode[K, V]
	curr *SLNode[K, V]
	list *SkipList[K, V]
}

// HasNext implements Iterator.
func (s *slIterator[K, V]) HasNext() bool {
	return s.curr != nil && s.curr.Next() != nil
}

// Next implements Iterator.
func (s *slIterator[K, V]) Next() (V, error) {
	if !s.HasNext() {
		var empty V
		return empty, EOI
	}
	s.curr = s.curr.Next()
	return s.curr.Value, nil
}

// HasPrev implements Iterator.
func (s *slIterator[K, V]) HasPrev() bool {
	return s.curr != nil && s.curr != s.head
}

// Prev implements Iterator.
func (s *slIterator[K, V]) Prev() (V, error) {
	if !s.HasPrev() {
		var empty V
		return empty, EOI
	}
	value := s.curr.Value
	s.curr = s.curr.backward
	return value, nil
}

// Last implements Iterator.
func (s *slIterator[K, V]) Last() (V, error) {
	var empty V
	if s.list == nil || s.list.tail == nil {
		return empty, EOI
	}
	tail := s.list.tail
	s.curr = tail.backward
	return tail.Value, nil
}

// Iterator returns a bidirectional iterator over the list's values.
func (list *SkipList[K, V]) Iterator() Iterator[V] {
	h := list.Head()
	return &slIterator[K, V]{
		head: h,
		curr: h,
		list: list,
	}
}

// slIRange iterates over a key range within the skip list.
type slIRange[K Comparable, V any] struct {
	curr     *SLNode[K, V]
	startKey K
	endKey   K
	list     *SkipList[K, V]
	order    RangeOrder
	desc     *SLNode[K, V]
}

// HasNext implements Iterator.
func (s *slIRange[K, V]) HasNext() bool {
	return s.curr != nil && s.curr.Next() != nil && Compare(s.curr.Next().Key, s.endKey) != CmpGreater
}

// Next implements Iterator.
func (s *slIRange[K, V]) Next() (V, error) {
	if !s.HasNext() {
		var empty V
		return empty, EOI
	}
	s.curr = s.curr.Next()
	return s.curr.Value, nil
}

// HasPrev implements Iterator.
func (s *slIRange[K, V]) HasPrev() bool {
	if s.order == RangeDesc {
		if s.desc == nil {
			return false
		}
		if Compare(s.desc.Key, s.startKey) == CmpLess {
			s.desc = nil
			return false
		}
		return true
	}
	return s.curr != nil && Compare(s.curr.Key, s.startKey) != CmpLess
}

// Prev implements Iterator.
func (s *slIRange[K, V]) Prev() (V, error) {
	if s.order == RangeDesc {
		if !s.HasPrev() {
			var empty V
			return empty, EOI
		}
		node := s.desc
		value := node.Value
		prev := node.backward
		if prev != nil && Compare(prev.Key, s.startKey) == CmpLess {
			prev = nil
		}
		s.curr = prev
		s.desc = prev
		return value, nil
	}
	if !s.HasPrev() {
		var empty V
		return empty, EOI
	}
	value := s.curr.Value
	s.curr = s.curr.backward
	return value, nil
}

// Last implements Iterator.
func (s *slIRange[K, V]) Last() (V, error) {
	var empty V
	if s.list == nil {
		return empty, EOI
	}
	node, ok := s.list.findLessOrEqual(s.endKey)
	if !ok {
		return empty, EOI
	}
	if Compare(node.Key, s.startKey) == CmpLess {
		return empty, EOI
	}
	s.curr = node.backward
	if s.order == RangeDesc {
		prev := node.backward
		if prev != nil && Compare(prev.Key, s.startKey) == CmpLess {
			prev = nil
		}
		s.desc = prev
	}
	return node.Value, nil
}

// IRange returns an iterator over records with keys in [start, end].
func (list *SkipList[K, V]) IRange(start, end K, order RangeOrder) Iterator[V] {
	rn := list.Head()
	rl := list.level
	for rl > 0 {
		rl--
		for rn.forwards[rl] != nil && Compare(rn.forwards[rl].Key, start) == CmpLess {
			rn = rn.forwards[rl]
		}
	}
	curr := rn
	var desc *SLNode[K, V]
	if order == RangeDesc {
		if node, ok := list.findLessOrEqual(end); ok && Compare(node.Key, start) != CmpLess {
			desc = node
		}
	}
	return &slIRange[K, V]{
		curr:     curr,
		startKey: start,
		endKey:   end,
		list:     list,
		order:    order,
		desc:     desc,
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
