package rindb

import (
	"context"
	"errors"
	"io"
	"sort"
)

var _ Iterator[Record] = (*sstableIterator)(nil)

func (s SStable) Iterator() (Iterator[Record], error) {
	if !s.IsOpened() {
		if err := s.Open(context.Background()); err != nil {
			return nil, err
		}
	}

	return &sstableIterator{
		currentIdx: 0,
		maxIdx:     len(s.SparseIndex),
		FileSystem: s.FileSystem,
		offset:     0,
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

	return &sstableIRange{s: &s, startKey: start, endKey: end, seq: maxSeq, offset: startOffset, dataEnd: dataEnd}, nil
}

// IRangeReverse returns an iterator over records in [start, end] in descending
// key order.
func (s SStable) IRangeReverse(start, end Bytes, seq ...uint64) (BiIterator[Record], error) {
	maxSeq := getMaxSeq(seq...)
	if !s.IsOpened() {
		if err := s.Open(context.Background()); err != nil {
			return nil, err
		}
	}
	idx := sort.Search(len(s.SparseIndex), func(i int) bool {
		return s.SparseIndex[i].key.Compare(end) > 0
	})
	var offset int64
	if idx > 0 {
		idx--
		offset = s.SparseIndex[idx].offset
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

	return &sstableIRangeRev{s: &s, startKey: start, endKey: end, seq: maxSeq, blockIdx: idx, offset: offset, dataEnd: dataEnd, pos: -1}, nil
}

type sstableIterator struct {
	*FileSystem
	currentIdx int
	maxIdx     int
	offset     int64
}

// HasNext implements Iterator.
func (s *sstableIterator) HasNext() bool {
	return s.currentIdx < s.maxIdx
}

// Next implements Iterator.
func (s *sstableIterator) Next() (Record, error) {
	if s.HasNext() {
		reader := newOffsetReader(s.FileSystem, s.offset)
		record, err := readRecord(reader)
		if err != nil {
			return nil, err
		}
		s.offset = reader.Offset()
		s.currentIdx++
		return record, nil
	}
	return nil, EOI
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
}

func (sri *sstableIRange) prepare() {
	for !sri.prepared && sri.err == nil {
		if sri.offset >= sri.dataEnd {
			sri.err = EOI
			return
		}
		reader := newOffsetReader(sri.s.FileSystem, sri.offset)
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

// sstableIRangeRev iterates over a range of keys in reverse order.
type sstableIRangeRev struct {
	s        *SStable
	startKey Bytes
	endKey   Bytes
	seq      uint64
	blockIdx int
	offset   int64
	dataEnd  int64
	offsets  []int64
	pos      int
	err      error
}

func (srr *sstableIRangeRev) fillBuf() {
	if srr.blockIdx < 0 {
		srr.err = EOI
		return
	}
	var nextOffset int64
	if srr.blockIdx+1 < len(srr.s.SparseIndex) {
		nextOffset = srr.s.SparseIndex[srr.blockIdx+1].offset
	} else {
		nextOffset = srr.dataEnd
	}
	reader := newOffsetReader(srr.s.FileSystem, srr.offset)
	var (
		keys []Bytes
		seqs []uint64
		offs []int64
	)
	for reader.Offset() < nextOffset {
		start := reader.Offset()
		key, seq, err := readRecordMeta(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			srr.err = err
			return
		}
		if key.Compare(srr.endKey) > 0 {
			break
		}
		keys = append(keys, key)
		seqs = append(seqs, seq)
		offs = append(offs, start)
	}
	left := sort.Search(len(keys), func(i int) bool { return keys[i].Compare(srr.startKey) >= 0 })
	right := sort.Search(len(keys), func(i int) bool { return keys[i].Compare(srr.endKey) > 0 })
	srr.offsets = srr.offsets[:0]
	for i := left; i < right; i++ {
		if seqs[i] <= srr.seq {
			srr.offsets = append(srr.offsets, offs[i])
		}
	}
	srr.pos = len(srr.offsets) - 1
	srr.blockIdx--
	if srr.blockIdx >= 0 {
		srr.offset = srr.s.SparseIndex[srr.blockIdx].offset
	}
}

func (srr *sstableIRangeRev) prepare() {
	for srr.pos < 0 && srr.err == nil {
		srr.fillBuf()
	}
}

// HasNext implements Iterator but always returns false for reverse iterator.
func (srr *sstableIRangeRev) HasNext() bool { return false }

// Next implements Iterator and always returns EOI.
func (srr *sstableIRangeRev) Next() (Record, error) {
	var empty Record
	return empty, EOI
}

// HasPrev implements BiIterator.
func (srr *sstableIRangeRev) HasPrev() bool {
	srr.prepare()
	return srr.pos >= 0
}

// Prev implements BiIterator.
func (srr *sstableIRangeRev) Prev() (Record, error) {
	if !srr.HasPrev() {
		var empty Record
		return empty, srr.err
	}
	off := srr.offsets[srr.pos]
	srr.pos--
	reader := newOffsetReader(srr.s.FileSystem, off)
	rec, err := readRecord(reader)
	if err != nil {
		srr.err = err
		return rec, err
	}
	return rec, nil
}
