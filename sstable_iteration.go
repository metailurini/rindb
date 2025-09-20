package rindb

import (
	"context"
	"errors"
	"fmt"
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
		FileSystem: s.FileSystem,
		dataEnd:    s.dataEnd,
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

type sstableIterator struct {
	*FileSystem
	offset  int64
	dataEnd int64
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
	reader := newOffsetReader(s.FileSystem, s.offset)
	record, _, err := readRecord(reader)
	fmt.Printf("sstableIterator.Next: readRecord returned rec=%s, err=%v\n", record, err)
	switch {
	case err == nil:
		// No error, continue processing
	case errors.Is(err, EOI), errors.Is(err, io.EOF):
		return nil, EOI
	default:
		return nil, err
	}
	s.offset = reader.Offset()
	fmt.Printf("sstableIterator.Next: Returning rec %s\n", record.GetKey())
	return record, nil
}

// HasPrev implements Iterator.
func (s *sstableIterator) HasPrev() bool {
	return s.offset > 0
}

// Prev implements Iterator.
func (s *sstableIterator) Prev() (Record, error) {
	if !s.HasPrev() {
		return nil, EOI
	}
	reader := newOffsetReader(s.FileSystem, s.offset)
	start, err := reader.PrevOffset()
	switch {
	case err == nil:
		// No error, continue processing
	case errors.Is(err, io.EOF):
		return nil, EOI
	default:
		return nil, err
	}
	reader.offset = start
	record, _, err := readRecord(reader)
	fmt.Printf("sstableIterator.Prev: readRecord returned rec=%s, err=%v\n", record, err)
	switch {
	case err == nil:
		// No error, continue processing
	case errors.Is(err, EOI), errors.Is(err, io.EOF):
		return nil, EOI
	default:
		return nil, err
	}
	s.offset = start
	fmt.Printf("sstableIterator.Prev: Returning rec %s\n", record.GetKey())
	return record, nil
}

// sstableIRange iterates over a range of keys in an SSTable.
type sstableIRange struct {
	s              *SStable
	startKey       Bytes
	endKey         Bytes
	seq            uint64
	offset         int64
	cursor         int64
	preparedOffset int64
	lowerBound     int64
	haveLowerBound bool
	dataEnd        int64
	next           Record
	err            error
	prepared       bool
}

func (sri *sstableIRange) prepare() {
	fmt.Printf("sstableIRange.prepare: start, offset=%d, dataEnd=%d, startKey=%s, endKey=%s, seq=%d\n", sri.offset, sri.dataEnd, sri.startKey, sri.endKey, sri.seq)
	for !sri.prepared && sri.err == nil {
		if sri.offset >= sri.dataEnd {
			sri.err = EOI
			return
		}
		cur := sri.offset
		reader := newOffsetReader(sri.s.FileSystem, cur)
		rec, _, err := readRecord(reader)
		fmt.Printf("sstableIRange.prepare: readRecord returned rec=%s, err=%v\n", rec, err)
		switch {
		case err == nil:
			// No error, continue processing
		case errors.Is(err, io.EOF):
			sri.err = EOI
			return
		default:
			sri.err = err
			return
		}
		sri.offset = reader.Offset()
		if rec.GetKey().Compare(sri.startKey) < 0 {
			fmt.Printf("sstableIRange.prepare: Skipping rec %s due to startKey filter\n", rec.GetKey())
			continue
		}
		if rec.GetKey().Compare(sri.endKey) > 0 {
			fmt.Printf("sstableIRange.prepare: Skipping rec %s due to endKey filter\n", rec.GetKey())
			sri.err = EOI
			return
		}
		if rec.GetSequenceNumber() > sri.seq {
			fmt.Printf("sstableIRange.prepare: Skipping rec %s due to sequence number filter\n", rec.GetKey())
			continue
		}
		sri.preparedOffset = cur
		sri.next = rec
		sri.prepared = true
		fmt.Printf("sstableIRange.prepare: Prepared next rec %s\n", sri.next.GetKey())
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
	if !sri.haveLowerBound {
		sri.lowerBound = sri.preparedOffset
		sri.haveLowerBound = true
	}
	sri.cursor = sri.offset
	sri.prepared = false
	next := sri.next
	sri.next = nil
	fmt.Printf("sstableIRange.Next: Returning rec %s\n", next.GetKey())
	return next, nil
}

// HasPrev implements Iterator.
func (sri *sstableIRange) HasPrev() bool {
	if !sri.haveLowerBound {
		return false
	}
	return sri.cursor > sri.lowerBound
}

// Prev implements Iterator.
func (sri *sstableIRange) Prev() (Record, error) {
	if !sri.HasPrev() {
		var empty Record
		return empty, EOI
	}
	reader := newOffsetReader(sri.s.FileSystem, sri.cursor)
	start, err := reader.PrevOffset()
	switch {
	case err == nil:
		// No error, continue processing
	case errors.Is(err, io.EOF):
		var empty Record
		return empty, EOI
	default:
		return nil, err
	}
	reader.offset = start
	rec, _, err := readRecord(reader)
	switch {
	case err == nil:
		// No error, continue processing
	case errors.Is(err, EOI), errors.Is(err, io.EOF):
		var empty Record
		return empty, EOI
	default:
		return nil, err
	}
	sri.offset = start
	sri.cursor = start
	sri.prepared = false
	sri.err = nil
	sri.next = nil
	fmt.Printf("sstableIRange.Prev: Returning rec %s\n", rec.GetKey())
	return rec, nil
}
