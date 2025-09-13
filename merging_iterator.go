package rindb

type pqItem struct {
	rec  Record
	iter BiIterator[Record]
}

// MergingIterator merges multiple iterators without deduplication. It yields
// records ordered by key and sequence number (descending), optionally in
// reverse key order when configured.
type MergingIterator struct {
	pq       *PriorityQueue[pqItem]
	next     Record
	prepared bool
	cleanup  func()
	err      error
	reverse  bool
}

// NewMergingIterator constructs a MergingIterator over provided iterators.
// If reverse is true, records are yielded in descending key order. The optional
// cleanup function is called when Close is invoked.
func NewMergingIterator(iterators []BiIterator[Record], reverse bool, cleanup func()) (*MergingIterator, error) {
	less := func(a, b pqItem) bool {
		cmp := a.rec.GetKey().Compare(b.rec.GetKey())
		if cmp == CmpEqual {
			return a.rec.GetSequenceNumber() > b.rec.GetSequenceNumber()
		}
		if reverse {
			return cmp == CmpGreater
		}
		return cmp == CmpLess
	}

	pq := NewPriorityQueue(less)
	for _, it := range iterators {
		var (
			rec Record
			err error
		)
		if reverse {
			if !it.HasPrev() {
				continue
			}
			rec, err = it.Prev()
		} else {
			if !it.HasNext() {
				continue
			}
			rec, err = it.Next()
		}
		if err != nil {
			if cleanup != nil {
				cleanup()
			}
			return nil, err
		}
		pq.PushItem(pqItem{rec: rec, iter: it})
	}

	return &MergingIterator{pq: pq, cleanup: cleanup, reverse: reverse}, nil
}

func (m *MergingIterator) prepare() {
	if m.prepared || m.err != nil || m.pq.Len() == 0 {
		return
	}

	item := m.pq.PopItem()
	m.next = item.rec
	m.prepared = true

	var (
		rec Record
		err error
	)
	if m.reverse {
		if item.iter.HasPrev() {
			rec, err = item.iter.Prev()
		} else {
			return
		}
	} else {
		if item.iter.HasNext() {
			rec, err = item.iter.Next()
		} else {
			return
		}
	}
	if err != nil {
		m.err = err
	} else {
		m.pq.PushItem(pqItem{rec: rec, iter: item.iter})
	}
}

// HasNext implements Iterator[Record].
func (m *MergingIterator) HasNext() bool {
	m.prepare()
	return m.prepared
}

// Next implements Iterator[Record].
func (m *MergingIterator) Next() (Record, error) {
	if !m.HasNext() {
		var empty Record
		if m.err != nil {
			return empty, m.err
		}
		return empty, EOI
	}
	m.prepared = false
	return m.next, nil
}

// Close releases any resources held by the iterator. It is safe to call
// multiple times.
func (m *MergingIterator) Close() error {
	if m.cleanup != nil {
		m.cleanup()
		m.cleanup = nil
	}
	return m.err
}
