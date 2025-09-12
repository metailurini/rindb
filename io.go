package rindb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

var bufPool = sync.Pool{New: func() any {
	b := make([]byte, 1<<16)
	return &b
}}

func getBuf(n int) []byte {
	p := bufPool.Get().(*[]byte)
	if cap(*p) < n {
		b := make([]byte, n)
		*p = b
	}
	return (*p)[:n]
}

func putBuf(b []byte) { bufPool.Put(&b) }

const (
	mdByteSize   = 8
	checksumSize = 4
)

var ErrChecksumMismatch = errors.New("checksum mismatch")

func readNumber(storage io.Reader) (uint64, error) {
	numBytes := [mdByteSize]byte{}
	if _, err := io.ReadFull(storage, numBytes[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(numBytes[:]), nil
}

func readRecord(storage io.Reader) (Record, error) {
	internalKeyLen, err := readNumber(storage)
	if err != nil {
		return nil, fmt.Errorf("failed to read internal key length: %w", err)
	}

	valueLen, err := readNumber(storage)
	if err != nil {
		return nil, fmt.Errorf("failed to read value length: %w", err)
	}

	var internalKeyBytes Bytes
	if internalKeyLen > 0 {
		ikey := getBuf(int(internalKeyLen))
		defer putBuf(ikey)
		if _, err := io.ReadFull(storage, ikey[:internalKeyLen]); err != nil {
			return nil, fmt.Errorf("failed to read internal key bytes: %w", err)
		}
		internalKeyBytes = make(Bytes, internalKeyLen)
		copy(internalKeyBytes, ikey[:internalKeyLen])
	}

	userKey, seq, typ, err := DecodeInternalKey(internalKeyBytes)
	if err != nil {
		return nil, err
	}

	var valueBytes Bytes
	if valueLen > 0 {
		val := getBuf(int(valueLen))
		defer putBuf(val)
		if _, err := io.ReadFull(storage, val[:valueLen]); err != nil {
			return nil, fmt.Errorf("failed to read value bytes: %w", err)
		}
		valueBytes = make(Bytes, valueLen)
		copy(valueBytes, val[:valueLen])
	}

	var checksumBytes [checksumSize]byte
	if _, err := io.ReadFull(storage, checksumBytes[:]); err != nil {
		return nil, fmt.Errorf("failed to read checksum: %w", err)
	}
	expected := binary.BigEndian.Uint32(checksumBytes[:])
	if actual := checksum(internalKeyBytes, valueBytes); actual != expected {
		return nil, ErrChecksumMismatch
	}

	return RecordImpl{
		Key:            userKey,
		Value:          valueBytes,
		SequenceNumber: seq,
		Type:           typ,
	}, nil
}

func writeNumber(tx *transaction, number uint64) error {
	numBytes := [mdByteSize]byte{}
	binary.BigEndian.PutUint64(numBytes[:], number)
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
	binary.BigEndian.PutUint32(checksumBytes[:], checksum)
	if _, err := tx.write(checksumBytes[:]); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	return nil
}
