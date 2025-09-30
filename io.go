package rindb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Use BigEndian for consistent cross-platform encoding/decoding
var byteOrder = binary.BigEndian

const (
	mdByteSize   = 8
	checksumSize = 4
)

type zeroCopyPeekReader interface {
	peekSlice(int) ([]byte, bool)
}

type meteredReader struct {
	r io.Reader
	n int
}

func newMeteredReader(r io.Reader) *meteredReader {
	return &meteredReader{r: r}
}

func (mr *meteredReader) Read(p []byte) (int, error) {
	n, err := mr.r.Read(p)
	mr.n += n
	return n, err
}

func (mr *meteredReader) BytesRead() int {
	return mr.n
}

var ErrChecksumMismatch = errors.New("checksum mismatch")

func readNumber(storage io.Reader) (uint64, error) {
	numBytes := [mdByteSize]byte{}
	if _, err := io.ReadFull(storage, numBytes[:]); err != nil {
		return 0, err
	}
	return byteOrder.Uint64(numBytes[:]), nil
}

func readRecord(storage io.Reader) (Record, int, error) {
	if zr, ok := storage.(zeroCopyPeekReader); ok {
		if record, size, err, used := readRecordZeroCopy(zr); used {
			return record, size, err
		}
	}

	return readRecordCopy(storage)
}

func readRecordCopy(storage io.Reader) (Record, int, error) {
	reader := newMeteredReader(storage)

	internalKeyLen, err := readNumber(reader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read internal key length: %w", err)
	}

	valueLen, err := readNumber(reader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read value length: %w", err)
	}

	var internalKeyBytes Bytes
	if internalKeyLen > 0 {
		internalKeyBytes = make(Bytes, internalKeyLen)
		if _, err := io.ReadFull(reader, internalKeyBytes); err != nil {
			return nil, 0, fmt.Errorf("failed to read internal key bytes: %w", err)
		}
	}

	userKey, seq, typ, err := DecodeInternalKey(internalKeyBytes)
	if err != nil {
		return nil, 0, err
	}

	var valueBytes Bytes
	if valueLen > 0 {
		valueBytes = make(Bytes, valueLen)
		if _, err := io.ReadFull(reader, valueBytes); err != nil {
			return nil, 0, fmt.Errorf("failed to read value bytes: %w", err)
		}
	}

	var checksumBytes [checksumSize]byte
	if _, err := io.ReadFull(reader, checksumBytes[:]); err != nil {
		return nil, 0, fmt.Errorf("failed to read checksum: %w", err)
	}
	expected := byteOrder.Uint32(checksumBytes[:])
	if actual := checksum(internalKeyBytes, valueBytes); actual != expected {
		return nil, 0, ErrChecksumMismatch
	}

	trailer, err := readNumber(reader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read record size trailer: %w", err)
	}
	size := reader.BytesRead()
	if trailer != uint64(size) {
		return nil, 0, fmt.Errorf("record size mismatch: got %d expect %d", trailer, size)
	}

	return RecordImpl{
		Key:            userKey,
		Value:          valueBytes,
		SequenceNumber: seq,
		Type:           typ,
	}, size, nil
}

func readRecordZeroCopy(r zeroCopyPeekReader) (Record, int, error, bool) {
	internalKeyLenBytes, ok := r.peekSlice(mdByteSize)
	if !ok {
		return nil, 0, nil, false
	}
	total := mdByteSize
	internalKeyLen := byteOrder.Uint64(internalKeyLenBytes)

	valueLenBytes, ok := r.peekSlice(mdByteSize)
	if !ok {
		return nil, total, fmt.Errorf("failed to read value length: %w", io.ErrUnexpectedEOF), true
	}
	total += mdByteSize
	valueLen := byteOrder.Uint64(valueLenBytes)

	var internalKeyBytes Bytes
	if internalKeyLen > 0 {
		slice, ok := r.peekSlice(int(internalKeyLen))
		if !ok {
			return nil, total, fmt.Errorf("failed to read internal key bytes: %w", io.ErrUnexpectedEOF), true
		}
		total += int(internalKeyLen)
		internalKeyBytes = Bytes(slice)
	}

	userKey, seq, typ, err := DecodeInternalKey(internalKeyBytes)
	if err != nil {
		return nil, total, err, true
	}

	var valueBytes Bytes
	if valueLen > 0 {
		slice, ok := r.peekSlice(int(valueLen))
		if !ok {
			return nil, total, fmt.Errorf("failed to read value bytes: %w", io.ErrUnexpectedEOF), true
		}
		total += int(valueLen)
		valueBytes = Bytes(slice)
	}

	checksumSlice, ok := r.peekSlice(checksumSize)
	if !ok {
		return nil, total, fmt.Errorf("failed to read checksum: %w", io.ErrUnexpectedEOF), true
	}
	total += checksumSize
	expected := byteOrder.Uint32(checksumSlice)
	if actual := checksum(internalKeyBytes, valueBytes); actual != expected {
		return nil, total, ErrChecksumMismatch, true
	}

	trailerSlice, ok := r.peekSlice(mdByteSize)
	if !ok {
		return nil, total, fmt.Errorf("failed to read record size trailer: %w", io.ErrUnexpectedEOF), true
	}
	total += mdByteSize
	trailer := byteOrder.Uint64(trailerSlice)
	if trailer != uint64(total) {
		return nil, total, fmt.Errorf("record size mismatch: got %d expect %d", trailer, total), true
	}

	return RecordImpl{
		Key:            userKey,
		Value:          valueBytes,
		SequenceNumber: seq,
		Type:           typ,
	}, total, nil, true
}

func writeNumber(tx *transaction, number uint64) error {
	numBytes := [mdByteSize]byte{}
	byteOrder.PutUint64(numBytes[:], number)
	if _, err := tx.write(numBytes[:]); err != nil {
		return fmt.Errorf("failed to write number bytes: %w", err)
	}
	return nil
}

func writeRecord(tx *transaction, record Record) error {
	ikey := EncodeInternalKey(record.GetKey(), record.GetSequenceNumber(), record.GetType())
	val := record.GetValue()

	checksum := checksum(ikey, val)

	if err := writeNumber(tx, uint64(len(ikey))); err != nil {
		return fmt.Errorf("failed to write internal key length: %w", err)
	}

	if err := writeNumber(tx, uint64(len(val))); err != nil {
		return fmt.Errorf("failed to write value length: %w", err)
	}

	if _, err := tx.write(ikey); err != nil {
		return fmt.Errorf("failed to write internal key bytes: %w", err)
	}

	if _, err := tx.write(val); err != nil {
		return fmt.Errorf("failed to write value bytes: %w", err)
	}

	var checksumBytes [checksumSize]byte
	byteOrder.PutUint32(checksumBytes[:], checksum)
	if _, err := tx.write(checksumBytes[:]); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	if err := writeNumber(tx, uint64(CalOnDiskSize(record))); err != nil {
		return fmt.Errorf("failed to write record size trailer: %w", err)
	}

	return nil
}
