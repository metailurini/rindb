package rindb

type Snapshot struct {
	seq int64
}

var Latest = Snapshot{seq: -1}

func (s *Snapshot) Seq() int64 {
	if s == nil {
		return Latest.seq
	}
	return s.seq
}

type snapshotIterator struct {
	inner     Iterator[Record]
	seq       uint64
	next      Record
	nextValid bool
	lastKey   Bytes
}

func newSnapshotIterator(inner Iterator[Record], seq uint64) Iterator[Record] {
	it := &snapshotIterator{inner: inner, seq: seq}
	it.advance()
	return it
}

func (it *snapshotIterator) advance() {
	for it.inner.HasNext() {
		rec, err := it.inner.Next()
		if err != nil {
			continue
		}
		if rec.GetSequenceNumber() > it.seq {
			continue
		}
		k := rec.GetKey()
		if it.lastKey != nil && it.lastKey.Compare(k) == CmpEqual {
			continue
		}
		it.lastKey = k
		if rec.GetValue() == nil {
			continue
		}
		it.next = rec
		it.nextValid = true
		return
	}
	it.nextValid = false
}

func (it *snapshotIterator) HasNext() bool {
	return it.nextValid
}

func (it *snapshotIterator) Next() (Record, error) {
	if !it.nextValid {
		return nil, EOI
	}
	rec := it.next
	it.advance()
	return rec, nil
}
