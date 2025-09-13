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
	blockIdx := idx - 1
	var blockStart int64
	if blockIdx >= 0 {
		blockStart = s.SparseIndex[blockIdx].offset
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

	var scanEnd int64
	if idx < len(s.SparseIndex) {
		scanEnd = s.SparseIndex[idx].offset
	} else {
		scanEnd = dataEnd
	}

	fwd, err := s.IRange(start, end, maxSeq)
	if err != nil {
		return nil, err
	}

	return &sstableIRangeRev{
		s:          &s,
		startKey:   start,
		endKey:     end,
		seq:        maxSeq,
		blockIdx:   blockIdx,
		blockStart: blockStart,
		scanEnd:    scanEnd,
		dataEnd:    dataEnd,
		fwd:        fwd,
	}, nil
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

// sstableIRangeRev iterates over a range of keys in reverse order while also
// supporting forward iteration via a separate iterator.
type sstableIRangeRev struct {
	s          *SStable
	startKey   Bytes
	endKey     Bytes
	seq        uint64
	blockIdx   int
	blockStart int64
	scanEnd    int64
	dataEnd    int64
	cur        recMeta
	curValid   bool
	err        error
	fwd        Iterator[Record]
}

type recMeta struct {
	key      Bytes
	seq      uint64
	typ      RecordType
	valueOff int64
	valueLen uint64
}

func (srr *sstableIRangeRev) scanBlockForPrev(start, end int64) (recMeta, int64, bool, error) {
	reader := newOffsetReader(srr.s.FileSystem, start)
	var last recMeta
	lastStart := int64(-1)
	for reader.Offset() < end {
		recStart := reader.Offset()
		key, seq, typ, valueOff, valueLen, err := readRecordMeta(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return recMeta{}, 0, false, err
		}
		if key.Compare(srr.endKey) > 0 {
			break
		}
		if key.Compare(srr.startKey) >= 0 && seq <= srr.seq {
			last = recMeta{key: key, seq: seq, typ: typ, valueOff: valueOff, valueLen: valueLen}
			lastStart = recStart
		}
	}
	if lastStart == -1 {
		return recMeta{}, 0, false, nil
	}
	return last, lastStart, true, nil
}

func (srr *sstableIRangeRev) prepare() {
	for !srr.curValid && srr.err == nil {
		if srr.blockIdx < 0 {
			srr.err = EOI
			return
		}
		meta, off, ok, err := srr.scanBlockForPrev(srr.blockStart, srr.scanEnd)
		if err != nil {
			srr.err = err
			return
		}
		if ok {
			srr.cur = meta
			srr.curValid = true
			srr.scanEnd = off
			continue
		}
		srr.blockIdx--
		if srr.blockIdx < 0 {
			srr.err = EOI
			return
		}
		srr.blockStart = srr.s.SparseIndex[srr.blockIdx].offset
		if srr.blockIdx+1 < len(srr.s.SparseIndex) {
			srr.scanEnd = srr.s.SparseIndex[srr.blockIdx+1].offset
		} else {
			srr.scanEnd = srr.dataEnd
		}
	}
}

// HasNext implements Iterator by delegating to the forward iterator.
func (srr *sstableIRangeRev) HasNext() bool { return srr.fwd.HasNext() }

// Next implements Iterator by delegating to the forward iterator.
func (srr *sstableIRangeRev) Next() (Record, error) { return srr.fwd.Next() }

// HasPrev implements BiIterator.
func (srr *sstableIRangeRev) HasPrev() bool {
	srr.prepare()
	return srr.curValid
}

// Prev implements BiIterator.
func (srr *sstableIRangeRev) Prev() (Record, error) {
	if !srr.HasPrev() {
		var empty Record
		return empty, srr.err
	}
	meta := srr.cur
	srr.curValid = false
	reader := newOffsetReader(srr.s.FileSystem, meta.valueOff)
	var value Bytes
	if meta.valueLen > 0 {
		value = make(Bytes, meta.valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			srr.err = err
			return nil, err
		}
	}
	var checksumBytes [checksumSize]byte
	if _, err := io.ReadFull(reader, checksumBytes[:]); err != nil {
		srr.err = err
		return nil, err
	}
	ikey := EncodeInternalKey(meta.key, meta.seq, meta.typ)
	if checksum(ikey, value) != byteOrder.Uint32(checksumBytes[:]) {
		srr.err = ErrChecksumMismatch
		return nil, ErrChecksumMismatch
	}
	return RecordImpl{Key: meta.key, Value: value, SequenceNumber: meta.seq, Type: meta.typ}, nil
}
