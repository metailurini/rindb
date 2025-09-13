package rindb

import (
	"context"
	"errors"
	"io"
	"sort"
)

const maxSSTableIteratorOffsets = 1 << 16

type offsetStack struct {
	buf   []int64
	start int
	count int
}

func newOffsetStack(n int) *offsetStack {
	return &offsetStack{buf: make([]int64, n)}
}

func (o *offsetStack) push(v int64) {
	if len(o.buf) == 0 {
		return
	}
	if o.count < len(o.buf) {
		idx := (o.start + o.count) % len(o.buf)
		o.buf[idx] = v
		o.count++
		return
	}
	o.buf[o.start] = v
	o.start = (o.start + 1) % len(o.buf)
}

func (o *offsetStack) pop() (int64, bool) {
	if o.count == 0 {
		return 0, false
	}
	idx := (o.start + o.count - 1) % len(o.buf)
	v := o.buf[idx]
	o.count--
	return v, true
}

func (o *offsetStack) len() int { return o.count }

var _ Iterator[Record] = (*sstableIterator)(nil)

func (s SStable) Iterator() (Iterator[Record], error) {
	if !s.IsOpened() {
		if err := s.Open(context.Background()); err != nil {
			return nil, err
		}
	}

	return &sstableIterator{
		FileSystem: s.FileSystem,
		dataEnd:    s.dataEnd,
		offset:     0,
		offs:       newOffsetStack(maxSSTableIteratorOffsets),
	}, nil
}

// IRange returns an iterator over records with keys in [start, end] and sequence
// numbers less than or equal to seq.
func (s SStable) IRange(start, end Bytes, seq ...uint64) (Iterator[Record], error) {
	maxSeq := getMaxSeq(seq...)
	if !s.IsOpened() {
		if err := s.Open(context.Background()); err != nil {
			return nil, err
		}
	}
	startIdx := sort.Search(len(s.SparseIndex), func(i int) bool {
		return s.SparseIndex[i].key.Compare(start) >= 0
	})
	var startOffset int64
	switch {
	case startIdx < len(s.SparseIndex) && s.SparseIndex[startIdx].key.Compare(start) == CmpEqual:
		startOffset = s.SparseIndex[startIdx].offset
	case startIdx > 0:
		startOffset = s.SparseIndex[startIdx-1].offset
	}
	tailOffset, err := getSSTableTailOffset(s.FileSystem)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, mdByteSize)
	if _, err := s.FileSystem.ReadAt(buf, tailOffset); err != nil {
		return nil, err
	}
	dataEnd := int64(byteOrder.Uint64(buf))

	return &sstableIRange{s: &s, startKey: start, endKey: end, seq: maxSeq, offset: startOffset, dataEnd: dataEnd, offs: newOffsetStack(maxSSTableIteratorOffsets)}, nil
}

type sstableIterator struct {
	*FileSystem
	offset  int64
	dataEnd int64
	offs    *offsetStack
}

// HasNext implements Iterator.
func (s *sstableIterator) HasNext() bool {
	return s.offset < s.dataEnd
}

// Next implements Iterator.
func (s *sstableIterator) Next() (Record, error) {
	if !s.HasNext() {
		return nil, EOI
	}
	s.offs.push(s.offset)
	reader := newOffsetReader(s.FileSystem, s.offset)
	record, err := readRecord(reader)
	if err != nil {
		return nil, err
	}
	s.offset = reader.Offset()
	return record, nil
}

// HasPrev implements Iterator.
func (s *sstableIterator) HasPrev() bool {
	return s.offs.len() > 0
}

// Prev implements Iterator.
func (s *sstableIterator) Prev() (Record, error) {
	if !s.HasPrev() {
		return nil, EOI
	}
	prev, _ := s.offs.pop()
	reader := newOffsetReader(s.FileSystem, prev)
	record, err := readRecord(reader)
	if err != nil {
		return nil, err
	}
	s.offset = prev
	return record, nil
}

// sstableIRange iterates over a range of keys in an SSTable.
type sstableIRange struct {
	s        *SStable
	startKey Bytes
	endKey   Bytes
	seq      uint64
	offset   int64
	dataEnd  int64
	next     Record
	err      error
	prepared bool
	offs     *offsetStack
}

func (sri *sstableIRange) prepare() {
	for !sri.prepared && sri.err == nil {
		if sri.offset >= sri.dataEnd {
			sri.err = EOI
			return
		}
		cur := sri.offset
		reader := newOffsetReader(sri.s.FileSystem, cur)
		rec, err := readRecord(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				sri.err = EOI
			} else {
				sri.err = err
			}
			return
		}
		sri.offset = reader.Offset()
		if rec.GetKey().Compare(sri.startKey) < 0 {
			continue
		}
		if rec.GetKey().Compare(sri.endKey) > 0 {
			sri.err = EOI
			return
		}
		if rec.GetSequenceNumber() > sri.seq {
			continue
		}
		sri.offs.push(cur)
		sri.next = rec
		sri.prepared = true
	}
}

// HasNext implements Iterator.
func (sri *sstableIRange) HasNext() bool {
	sri.prepare()
	return sri.prepared
}

// Next implements Iterator.
func (sri *sstableIRange) Next() (Record, error) {
	if !sri.HasNext() {
		var empty Record
		return empty, sri.err
	}
	sri.prepared = false
	return sri.next, nil
}

// HasPrev implements Iterator.
func (sri *sstableIRange) HasPrev() bool {
	return sri.offs.len() > 0
}

// Prev implements Iterator.
func (sri *sstableIRange) Prev() (Record, error) {
	if !sri.HasPrev() {
		var empty Record
		return empty, EOI
	}
	prev, _ := sri.offs.pop()
	reader := newOffsetReader(sri.s.FileSystem, prev)
	rec, err := readRecord(reader)
	if err != nil {
		return nil, err
	}
	sri.offset = prev
	sri.prepared = false
	sri.err = nil
	return rec, nil
}
