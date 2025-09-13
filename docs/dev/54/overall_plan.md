# Support Reverse Range Scanning

1. **Introduce bi-directional iterator contracts**
   - Extend the minimal `Iterator` to optionally move backwards.
```go
type BiIterator[T any] interface {
    HasNext() bool
    Next() (T, error)
    HasPrev() bool
    Prev() (T, error)
}
```
   - **Cursor semantics:** `Next` and `Prev` maintain independent cursors.
     Calling `Prev` after `Next` does **not** return the element preceding the
     last `Next` result; instead `Prev` iterates from the end of the range.  A
     `BiIterator` is therefore expected to be consumed in a single direction per
     use.
2. **Allow `MergingIterator` to order items descending**
   - Add a `reverse bool` flag and adjust the priority queue comparator.
```go
func NewMergingIterator(iterators []BiIterator[Record], reverse bool, cleanup func()) (*MergingIterator, error) {
    less := func(a, b pqItem) bool {
        cmp := a.rec.GetKey().Compare(b.rec.GetKey())
        if cmp == CmpEqual {
            return a.rec.GetSequenceNumber() > b.rec.GetSequenceNumber()
        }
        if reverse { return cmp == CmpGreater }
        return cmp == CmpLess
    }
    pq := NewPriorityQueue(less)
    // prime heap with first/last record depending on direction
    for _, it := range iterators {
        if reverse && it.HasPrev() {
            rec, _ := it.Prev()
            pq.PushItem(pqItem{rec: rec, iter: it})
        } else if it.HasNext() {
            rec, _ := it.Next()
            pq.PushItem(pqItem{rec: rec, iter: it})
        }
    }
    return &MergingIterator{pq: pq, reverse: reverse, cleanup: cleanup}, nil
}
```
3. **Implement reverse-range readers for Memtable and SSTable**
   - Provide `IRangeReverse` which positions iterators at the end key and walks backwards.
```go
// Memtable
func (m *memtable) IRangeReverse(start, end Bytes, seq uint64) BiIterator[Record] {
    startKey := InternalKey{UserKey: start, Seq: 0, Type: TypeMerge}
    endKey   := InternalKey{UserKey: end,   Seq: math.MaxUint64, Type: TypeValue}
    it := m.data.IRangeReverse(endKey, startKey)
    return &memtableIRange{it: it, seq: seq}
}

// SSTable
func (s SStable) IRangeReverse(start, end Bytes, seq ...uint64) (BiIterator[Record], error) {
    // locate initial block using sparse index then seek backwards
    idx := sort.Search(len(s.SparseIndex), func(i int) bool { return s.SparseIndex[i].key.Compare(end) > 0 })
    offset := s.SparseIndex[idx-1].offset
    return &sstableIRangeRev{s: &s, startKey: start, endKey: end, seq: maxSeq, offset: offset}, nil
}
```
4. **Expose descending scans in public APIs**
   - Add `IRangeReverse` to `Rindb` and `Snapshot`; reuse `RangeIterator` for both directions.
```go
func (r *Rindb) IRangeReverse(ctx context.Context, start, end Bytes, seq ...uint64) (*RangeIterator, error) {
    iterators := []BiIterator[Record]{r.Memtable.IRangeReverse(start, end, maxSeq)}
    // gather SSTables...
    mi, err := NewMergingIterator(iterators, true, cleanup)
    if err != nil { return nil, err }
    return NewRangeIterator(mi), nil
}
```
5. **Add comprehensive tests**
   - Validate backward traversal across memtable, SSTable, and high-level API.
```go
func TestIRangeReverse(t *testing.T) {
    db := openTestDB(t)
    // write keys a..e, then scan [e,a]
    iter, err := db.IRangeReverse(ctx, Bytes("a"), Bytes("e"))
    require.NoError(t, err)
    expect := []string{"e","d","c","b","a"}
    for _, k := range expect {
        r, _ := iter.Next()
        assert.Equal(t, k, string(r.GetKey()))
    }
    _, err = iter.Next()
    assert.ErrorIs(t, err, EOI)
}
```
