package rindb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Use BigEndian for consistent cross-platform encoding/decoding
var byteOrder = binary.BigEndian

const mdByteSize = 8

func ReadNumber(storage io.Reader) (uint64, error) {
	numBytes := [mdByteSize]byte{}
	if _, err := storage.Read(numBytes[:]); err != nil {
		return 0, err
	}

	return byteOrder.Uint64(numBytes[:]), nil
}

func encodeInternalKey(key Bytes, seq uint64, typ RecordType) []byte {
	internalKey := make([]byte, len(key)+internalKeySuffixLen)
	copy(internalKey, key)
	var seqBuf [seqNumBytes]byte
	byteOrder.PutUint64(seqBuf[:], ^seq)
	copy(internalKey[len(key):], seqBuf[:])
	internalKey[len(key)+seqNumBytes] = byte(typ)
	return internalKey
}

func decodeInternalKey(ikey Bytes) (Bytes, uint64, RecordType, error) {
	if len(ikey) < internalKeySuffixLen {
		return nil, 0, 0, fmt.Errorf("internal key too short: %d", len(ikey))
	}
	userKeyEnd := len(ikey) - internalKeySuffixLen
	seq := ^byteOrder.Uint64(ikey[userKeyEnd : userKeyEnd+seqNumBytes])
	typ := RecordType(ikey[len(ikey)-1])
	userKey := Bytes(ikey[:userKeyEnd])
	if userKeyEnd == 0 {
		userKey = nil
	}
	return userKey, seq, typ, nil
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

	keyBytes := bytes.NewBuffer(nil)

	const defaultReadStep = uint64(255)

	for internalKeyLen > 0 {
		step := min(internalKeyLen, defaultReadStep)
		internalKeyLen -= step

		tempBytes := make(Bytes, step)

		if _, err := io.ReadFull(storage, tempBytes); err != nil {
			return nil, fmt.Errorf("failed to read internal key bytes: %w", err)
		}

		if _, err := keyBytes.Write(tempBytes); err != nil {
			return nil, fmt.Errorf("failed to write internal key to buffer: %w", err)
		}
	}

	userKey, seq, typ, err := decodeInternalKey(keyBytes.Bytes())
	if err != nil {
		return nil, err
	}

	valueBytes := bytes.NewBuffer(nil)
	for valueLen > 0 {
		step := min(valueLen, defaultReadStep)
		valueLen -= step

		tempBytes := make(Bytes, step)

		if _, err := io.ReadFull(storage, tempBytes); err != nil {
			return nil, fmt.Errorf("failed to read value bytes: %w", err)
		}

		if _, err := valueBytes.Write(tempBytes); err != nil {
			return nil, fmt.Errorf("failed to write value to buffer: %w", err)
		}
	}

	return RecordImpl{
		Key:            userKey,
		Value:          valueBytes.Bytes(),
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
	ikey := encodeInternalKey(record.GetKey(), record.GetSequenceNumber(), record.GetType())

	if err := WriteNumber(tx, uint64(len(ikey))); err != nil {
		return fmt.Errorf("failed to write internal key length: %w", err)
	}

	if err := WriteNumber(tx, uint64(len(record.GetValue()))); err != nil {
		return fmt.Errorf("failed to write value length: %w", err)
	}

	if _, err := tx.Write(ikey); err != nil {
		return fmt.Errorf("failed to write internal key bytes: %w", err)
	}

	if _, err := tx.Write(record.GetValue()); err != nil {
		return fmt.Errorf("failed to write value bytes: %w", err)
	}

	return nil
}
