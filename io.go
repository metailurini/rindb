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

var ErrChecksumMismatch = errors.New("checksum mismatch")

func readNumber(storage io.Reader) (uint64, error) {
	numBytes := [mdByteSize]byte{}
	if _, err := io.ReadFull(storage, numBytes[:]); err != nil {
		return 0, err
	}
	return byteOrder.Uint64(numBytes[:]), nil
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

// readRecordMeta reads the internal key and returns metadata needed to later
// reconstruct the record without re-reading the key. The reader must be an
// *offsetReader so its offset can be advanced without reading the skipped
// value and checksum bytes.
func readRecordMeta(r *offsetReader) (key Bytes, seq uint64, typ RecordType, valueOff int64, valueLen uint64, err error) {
	internalKeyLen, err := readNumber(r)
	if err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("failed to read internal key length: %w", err)
	}

	valueLen, err = readNumber(r)
	if err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("failed to read value length: %w", err)
	}

	var internalKeyBytes Bytes
	if internalKeyLen > 0 {
		internalKeyBytes = make(Bytes, internalKeyLen)
		if _, err := io.ReadFull(r, internalKeyBytes); err != nil {
			return nil, 0, 0, 0, 0, fmt.Errorf("failed to read internal key bytes: %w", err)
		}
	}

	key, seq, typ, err = DecodeInternalKey(internalKeyBytes)
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}

	valueOff = r.Offset()
	r.offset += int64(valueLen) + checksumSize
	return key, seq, typ, valueOff, valueLen, nil
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

	return nil
}
