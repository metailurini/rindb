package rindb

import (
	"errors"
	"io"
	"sort"
)

var _ Iterator[Record] = (*sstableIterator)(nil)

func (s SStable) Iterator() (Iterator[Record], error) {
	return &sstableIterator{
		FileSystem: s.FileSystem,
		dataEnd:    s.dataEnd,
		cursor:     0,
	}, nil
}

// IRange returns an iterator over records with keys in [start, end] and sequence
// numbers less than or equal to seq.
func (s SStable) IRange(start, end Bytes, order RangeOrder, seq ...uint64) (Iterator[Record], error) {
	maxSeq := getMaxSeq(seq...)
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

	sri := &sstableIRange{
		s:           &s,
		startKey:    start,
		endKey:      end,
		seq:         maxSeq,
		offset:      startOffset,
		startOffset: startOffset,
		dataEnd:     dataEnd,
		order:       order,
	}

	if order == RangeDesc {
		if _, nextOffset, ok := s.findOffsetLE(end); ok {
			sri.offset = nextOffset
			sri.cursor = nextOffset
		} else {
			sri.offset = dataEnd
			sri.cursor = dataEnd
		}
	}

	return sri, nil
}

func (s SStable) findOffsetLE(end Bytes) (int64, int64, bool) {
	if len(s.SparseIndex) == 0 {
		return 0, 0, false
	}
	idx := sort.Search(len(s.SparseIndex), func(i int) bool {
		return s.SparseIndex[i].key.Compare(end) == CmpGreater
	})
	switch {
	case idx == 0:
		return 0, s.SparseIndex[0].offset, true
	case idx < len(s.SparseIndex):
		return s.SparseIndex[idx-1].offset, s.SparseIndex[idx].offset, true
	default:
		return s.SparseIndex[len(s.SparseIndex)-1].offset, s.dataEnd, true
	}
}

type sstableIterator struct {
	*FileSystem
	offset  int64
	cursor  int64
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
	switch {
	case err == nil:
		// No error, continue processing
	case errors.Is(err, EOI), errors.Is(err, io.EOF):
		return nil, EOI
	default:
		return nil, err
	}
	s.offset = reader.Offset()
	s.cursor = s.offset
	return record, nil
}

// HasPrev implements Iterator.
func (s *sstableIterator) HasPrev() bool {
	if s.cursor == 0 && s.offset > 0 {
		s.cursor = s.offset
	}
	return s.cursor > 0
}

// Prev implements Iterator.
func (s *sstableIterator) Prev() (Record, error) {
	if !s.HasPrev() {
		return nil, EOI
	}
	reader := newOffsetReader(s.FileSystem, s.cursor)
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
	switch {
	case err == nil:
		// No error, continue processing
	case errors.Is(err, EOI), errors.Is(err, io.EOF):
		return nil, EOI
	default:
		return nil, err
	}
	s.offset = start
	s.cursor = start
	return record, nil
}

// Last implements Iterator.
func (s *sstableIterator) Last() (Record, error) {
	reader := newOffsetReader(s.FileSystem, s.dataEnd)
	start, err := reader.PrevOffset()
	switch {
	case err == nil:
		// continue
	case errors.Is(err, io.EOF):
		return nil, EOI
	default:
		return nil, err
	}
	reader.offset = start
	record, _, err := readRecord(reader)
	switch {
	case err == nil:
		// continue
	case errors.Is(err, EOI), errors.Is(err, io.EOF):
		return nil, EOI
	default:
		return nil, err
	}
	s.cursor = start
	s.offset = reader.Offset()
	return record, nil
}

// sstableIRange iterates over a range of keys in an SSTable.
type sstableIRange struct {
	s                *SStable
	startKey         Bytes
	endKey           Bytes
	seq              uint64
	offset           int64
	cursor           int64
	startOffset      int64
	preparedOffset   int64
	lowerBound       int64
	haveLowerBound   bool
	dataEnd          int64
	next             Record
	err              error
	prepared         bool
	order            RangeOrder
	descendingPrimed bool
	replayNext       Record
	replayPrimed     bool
	replayCursor     int64
	prev             Record
	prevPrepared     bool
	prevErr          error
}

func (sri *sstableIRange) prepare() {
	for !sri.prepared && sri.err == nil {
		if sri.offset >= sri.dataEnd {
			sri.err = EOI
			return
		}
		cur := sri.offset
		reader := newOffsetReader(sri.s.FileSystem, cur)
		rec, _, err := readRecord(reader)
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
			continue
		}
		if rec.GetKey().Compare(sri.endKey) > 0 {
			sri.err = EOI
			return
		}
		if rec.GetSequenceNumber() > sri.seq {
			continue
		}
		sri.preparedOffset = cur
		sri.next = rec
		sri.prepared = true
	}
}

func (sri *sstableIRange) primeDescending() {
	if sri.descendingPrimed || sri.err != nil {
		return
	}
	if sri.offset == 0 && sri.cursor > 0 {
		sri.offset = sri.cursor
	}
	sri.descendingPrimed = true
}

func (sri *sstableIRange) primeNextDescending() {
	if sri.err != nil {
		return
	}
	if !sri.descendingPrimed {
		sri.primeDescending()
	}
	if sri.replayPrimed {
		sri.next = sri.replayNext
		sri.prepared = true
		sri.replayPrimed = false
		sri.replayNext = nil
		sri.preparedOffset = sri.offset
		sri.cursor = sri.replayCursor
		sri.replayCursor = 0
		return
	}
	for !sri.prepared && sri.err == nil {
		if sri.offset <= 0 {
			sri.err = EOI
			return
		}
		reader := newOffsetReader(sri.s.FileSystem, sri.offset)
		start, err := reader.PrevOffset()
		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			sri.err = EOI
			return
		default:
			sri.err = err
			return
		}
		reader.offset = start
		rec, _, err := readRecord(reader)
		switch {
		case err == nil:
		case errors.Is(err, EOI), errors.Is(err, io.EOF):
			sri.offset = reader.Offset()
			sri.cursor = start
			continue
		default:
			sri.err = err
			return
		}
		sri.offset = start
		sri.cursor = reader.Offset()
		key := rec.GetKey()
		if key.Compare(sri.endKey) == CmpGreater {
			continue
		}
		if key.Compare(sri.startKey) == CmpLess {
			sri.err = EOI
			return
		}
		if rec.GetSequenceNumber() > sri.seq {
			continue
		}
		sri.preparedOffset = start
		sri.next = rec
		sri.prepared = true
	}
}

// HasNext implements Iterator.
func (sri *sstableIRange) HasNext() bool {
	if sri.order == RangeDesc {
		if !sri.descendingPrimed {
			sri.primeDescending()
		}
		if !sri.prepared {
			sri.primeNextDescending()
		}
		return sri.prepared
	}
	sri.prepare()
	return sri.prepared
}

// Next implements Iterator.
func (sri *sstableIRange) Next() (Record, error) {
	if !sri.HasNext() {
		var empty Record
		return empty, sri.err
	}
	if !sri.haveLowerBound || sri.preparedOffset < sri.lowerBound {
		sri.lowerBound = sri.preparedOffset
		sri.haveLowerBound = true
	}
	if sri.order != RangeDesc {
		sri.cursor = sri.offset
	}
	sri.prepared = false
	next := sri.next
	sri.next = nil
	sri.prev = nil
	sri.prevPrepared = false
	sri.prevErr = nil
	return next, nil
}

func (sri *sstableIRange) ensureLowerBound() error {
	if sri.haveLowerBound {
		return nil
	}
	reader := newOffsetReader(sri.s.FileSystem, sri.startOffset)
	offset := sri.startOffset
	for offset < sri.dataEnd {
		cur := offset
		rec, _, err := readRecord(reader)
		switch {
		case err == nil:
			// continue
		case errors.Is(err, EOI), errors.Is(err, io.EOF):
			return EOI
		default:
			return err
		}
		offset = reader.Offset()
		if rec.GetKey().Compare(sri.startKey) < 0 {
			continue
		}
		if rec.GetKey().Compare(sri.endKey) > 0 {
			return EOI
		}
		if rec.GetSequenceNumber() > sri.seq {
			continue
		}
		sri.lowerBound = cur
		sri.haveLowerBound = true
		return nil
	}
	return EOI
}

func (sri *sstableIRange) primePrev() {
	if sri.prevPrepared || sri.prevErr != nil {
		return
	}
	if !sri.haveLowerBound {
		return
	}
	sri.prevErr = nil
	for !sri.prevPrepared {
		if sri.cursor <= sri.lowerBound {
			sri.prevErr = EOI
			return
		}
		reader := newOffsetReader(sri.s.FileSystem, sri.cursor)
		start, err := reader.PrevOffset()
		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			sri.prevErr = EOI
			return
		default:
			sri.prevErr = err
			return
		}
		reader.offset = start
		rec, _, err := readRecord(reader)
		switch {
		case err == nil:
		case errors.Is(err, EOI), errors.Is(err, io.EOF):
			sri.cursor = start
			sri.offset = start
			continue
		default:
			sri.prevErr = err
			return
		}

		sri.offset = start
		sri.cursor = start

		if rec.GetKey().Compare(sri.endKey) == CmpGreater {
			continue
		}
		if rec.GetKey().Compare(sri.startKey) == CmpLess {
			sri.prevErr = EOI
			return
		}
		if rec.GetSequenceNumber() > sri.seq {
			continue
		}

		sri.prev = rec
		sri.prevPrepared = true
		sri.prepared = false
		sri.err = nil
		sri.next = nil
		sri.replayNext = rec
		sri.replayPrimed = true
		sri.replayCursor = reader.Offset()
		return
	}
}

// HasPrev implements Iterator.
func (sri *sstableIRange) HasPrev() bool {
	if sri.prevPrepared {
		return true
	}
	if !sri.haveLowerBound {
		return false
	}
	return sri.cursor > sri.lowerBound
}

// Prev implements Iterator.
func (sri *sstableIRange) Prev() (Record, error) {
	var empty Record
	if !sri.prevPrepared {
		sri.primePrev()
	}
	if !sri.prevPrepared {
		if sri.prevErr != nil {
			return empty, sri.prevErr
		}
		return empty, EOI
        }
        rec := sri.prev
        sri.prev = nil
        sri.prevPrepared = false
        sri.prevErr = nil
        return rec, nil
}

// Last implements Iterator.
func (sri *sstableIRange) Last() (Record, error) {
	var empty Record
	sri.err = nil
	sri.prepared = false
	sri.next = nil
	sri.prev = nil
	sri.prevPrepared = false
	sri.prevErr = nil

	if err := sri.ensureLowerBound(); err != nil {
		return empty, err
	}

	cursor := sri.dataEnd
	if len(sri.s.SparseIndex) > 0 {
		idx := sort.Search(len(sri.s.SparseIndex), func(i int) bool {
			return sri.s.SparseIndex[i].key.Compare(sri.endKey) == CmpGreater
		})
		switch {
		case idx == 0:
			cursor = sri.s.SparseIndex[0].offset
		case idx < len(sri.s.SparseIndex):
			cursor = sri.s.SparseIndex[idx].offset
		default:
			cursor = sri.dataEnd
		}
	}

	sri.cursor = cursor
	sri.offset = cursor
	sri.replayNext = nil
	sri.replayPrimed = false
	sri.replayCursor = 0

	for {
		sri.primePrev()
		if sri.prevPrepared {
			rec := sri.prev
			sri.prev = nil
			sri.prevPrepared = false
			return rec, nil
		}
		if sri.prevErr != nil {
			return empty, sri.prevErr
		}
		return empty, EOI
	}
}
