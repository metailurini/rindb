package rindb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Use BigEndian for consistent cross-platform encoding/decoding
var byteOrder = binary.BigEndian

const (
	mdByteSize   = 8
	checksumSize = 4
)

var ErrChecksumMismatch = errors.New("checksum mismatch")

func ReadNumber(storage io.Reader) (uint64, error) {
	numBytes := [mdByteSize]byte{}
	if _, err := io.ReadFull(storage, numBytes[:]); err != nil {
		return 0, err
	}
	return byteOrder.Uint64(numBytes[:]), nil
}

func ReadRecord(storage io.Reader) (Record, error) {
	internalKeyLen, err := ReadNumber(storage)
	if err != nil {
		return nil, fmt.Errorf("failed to read internal key length: %w", err)
	}

	valueLen, err := ReadNumber(storage)
	if err != nil {
		return nil, fmt.Errorf("failed to read value length: %w", err)
	}

	var internalKeyBytes Bytes
	if internalKeyLen > 0 {
		internalKeyBytes = make(Bytes, internalKeyLen)
		if _, err := io.ReadFull(storage, internalKeyBytes); err != nil {
			return nil, fmt.Errorf("failed to read internal key bytes: %w", err)
		}
	}

	userKey, seq, typ, err := DecodeInternalKey(internalKeyBytes)
	if err != nil {
		return nil, err
	}

	var valueBytes Bytes
	if valueLen > 0 {
		valueBytes = make(Bytes, valueLen)
		if _, err := io.ReadFull(storage, valueBytes); err != nil {
			return nil, fmt.Errorf("failed to read value bytes: %w", err)
		}
	}

	var checksumBytes [checksumSize]byte
	if _, err := io.ReadFull(storage, checksumBytes[:]); err != nil {
		return nil, fmt.Errorf("failed to read checksum: %w", err)
	}
	expected := byteOrder.Uint32(checksumBytes[:])
	hasher := crc32.NewIEEE()
	_, _ = hasher.Write(internalKeyBytes)
	_, _ = hasher.Write(valueBytes)
	if actual := hasher.Sum32(); actual != expected {
		return nil, ErrChecksumMismatch
	}

	return RecordImpl{
		Key:            userKey,
		Value:          valueBytes,
		SequenceNumber: seq,
		Type:           typ,
	}, nil
}

func WriteNumber(tx *Transaction, number uint64) error {
	numBytes := [mdByteSize]byte{}
	byteOrder.PutUint64(numBytes[:], number)
	if _, err := tx.Write(numBytes[:]); err != nil {
		return fmt.Errorf("failed to write number bytes: %w", err)
	}
	return nil
}

func WriteRecord(tx *Transaction, record Record) error {
	ikey := EncodeInternalKey(record.GetKey(), record.GetSequenceNumber(), record.GetType())
	val := record.GetValue()

	hasher := crc32.NewIEEE()
	_, _ = hasher.Write(ikey)
	_, _ = hasher.Write(val)
	checksum := hasher.Sum32()

	if err := WriteNumber(tx, uint64(len(ikey))); err != nil {
		return fmt.Errorf("failed to write internal key length: %w", err)
	}

	if err := WriteNumber(tx, uint64(len(val))); err != nil {
		return fmt.Errorf("failed to write value length: %w", err)
	}

	if _, err := tx.Write(ikey); err != nil {
		return fmt.Errorf("failed to write internal key bytes: %w", err)
	}

	if _, err := tx.Write(val); err != nil {
		return fmt.Errorf("failed to write value bytes: %w", err)
	}

	var checksumBytes [checksumSize]byte
	byteOrder.PutUint32(checksumBytes[:], checksum)
	if _, err := tx.Write(checksumBytes[:]); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	return nil
}
