# Support reverse range scanning

Complexity is rated from **1** (trivial) to **10** (major cross-cutting change). Steps marked as requiring a dedicated plan file will be expanded separately.

1. **Extend the core Iterator contract** *(Complexity: 3/10)*
   ```go
   type Iterator[T any] interface {
       HasNext() bool
       Next() (T, error)
       HasPrev() bool
       Prev() (T, error)
   }
   ```

2. **Make SkipList/Memtable iterators bidirectional** *(Complexity: 5/10)*
   ```go
   type SLNode[K Comparable,V any] struct {
       Key   K
       Value V
       forwards []*SLNode[K,V]
       backward *SLNode[K,V] // level-0 link
   }

   type slIterator[K Comparable,V any] struct {
       curr *SLNode[K,V]
   }
   func (it *slIterator[K,V]) HasPrev() bool { return it.curr.backward != nil }
   func (it *slIterator[K,V]) Prev() (V, error) {
       if !it.HasPrev() { return *new(V), EOI }
       it.curr = it.curr.backward
       return it.curr.Value, nil
   }
   ```
   Maintain `backward` links in `Put`/`Remove` so the memtable iterator automatically gains `Prev`.

3. **Enable SSTable iterators to step backward without full buffering** *(Complexity: 8/10 — requires dedicated plan file)*
   ```go
   type sstableIterator struct {
       fs *FileSystem
       offsets []int64 // stack of visited record starts
       cur int64
   }
   func (s *sstableIterator) Next() (Record, error) {
       s.offsets = append(s.offsets, s.cur)
       r := newOffsetReader(s.fs, s.cur)
       rec, err := readRecord(r)
       s.cur = r.Offset()
       return rec, err
   }
   func (s *sstableIterator) HasPrev() bool { return len(s.offsets) > 0 }
   func (s *sstableIterator) Prev() (Record, error) {
       if !s.HasPrev() { return nil, EOI }
       prev := s.offsets[len(s.offsets)-1]
       s.offsets = s.offsets[:len(s.offsets)-1]
       r := newOffsetReader(s.fs, prev)
       rec, err := readRecord(r)
       s.cur = prev
       return rec, err
   }
   ```
   Apply the same technique to `sstableIRange`, storing only offsets already visited—no boolean flags or full-table buffering.

4. **Redesign MergingIterator & RangeIterator for two-way traversal** *(Complexity: 9/10 — requires dedicated plan file)*
   ```go
   type MergingIterator struct {
       fwd *PriorityQueue[pqItem] // min-heap by key,seq
       rev *PriorityQueue[pqItem] // max-heap
       cur pqItem
       err error
   }
   func (m *MergingIterator) Next() (Record, error) {
       m.cur = m.fwd.PopItem()
       m.rev.PushItem(m.cur)
       advanceForward(m.cur)
       return m.cur.rec, m.err
   }
   func (m *MergingIterator) Prev() (Record, error) {
       m.cur = m.rev.PopItem()
       m.fwd.PushItem(m.cur)
       advanceBackward(m.cur)
       return m.cur.rec, m.err
   }
   ```
   `RangeIterator` simply forwards `HasPrev`/`Prev` to the merging iterator while continuing to filter tombstones and duplicates.

5. **Update public APIs & tests** *(Complexity: 4/10)*
   ```go
   func TestRangeIteratorReverse(t *testing.T) {
       it := setupRangeIterator(t)
       var last Record
       for it.HasNext() { last, _ = it.Next() }
       for it.HasPrev() {
           rec, _ := it.Prev()
           // assert ordering and values
           last = rec
       }
   }
   ```
   Revise `Rindb.IRange`, `Memtable.Iterator`, and related tests to exercise forward and backward scans.

Dedicated plan files:

- Step 3: [sstable_iterator_plan.md](./sstable_iterator_plan.md)
- Step 4: [merging_range_iterators_plan.md](./merging_range_iterators_plan.md)
