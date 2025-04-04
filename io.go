package rindb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var byteOrder = binary.LittleEndian

const mdByteSize = 8

func ReadNumber(storage io.Reader) (uint64, error) {
	numBytes := [mdByteSize]byte{}
	if _, err := storage.Read(numBytes[:]); err != nil {
		return 0, err
	}

	return byteOrder.Uint64(numBytes[:]), nil
}

func ReadRecord(storage io.Reader) (Record, error) {
	keyLen, err := ReadNumber(storage)
	if err != nil {
		return nil, fmt.Errorf("failed to read key length: %w", err)
	}

	valueLen, err := ReadNumber(storage)
	if err != nil {
		return nil, fmt.Errorf("failed to read value length: %w", err)
	}

	keyBytes := bytes.NewBuffer(nil)

	const defaultReadStep = uint64(255)

	for keyLen > 0 {
		step := min(keyLen, defaultReadStep)
		keyLen -= step

		tempBytes := make(Bytes, step)

		if _, err := io.ReadFull(storage, tempBytes); err != nil {
			return nil, fmt.Errorf("failed to read key bytes: %w", err)
		}

		if _, err := keyBytes.Write(tempBytes); err != nil {
			return nil, fmt.Errorf("failed to write key to buffer: %w", err)
		}
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
		Key:   keyBytes.Bytes(),
		Value: valueBytes.Bytes(),
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
	if err := WriteNumber(tx, uint64(len(record.GetKey()))); err != nil {
		return fmt.Errorf("failed to write key length: %w", err)
	}

	if err := WriteNumber(tx, uint64(len(record.GetValue()))); err != nil {
		return fmt.Errorf("failed to write value length: %w", err)
	}

	if _, err := tx.Write(record.GetKey()); err != nil {
		return fmt.Errorf("failed to write key bytes: %w", err)
	}

	if _, err := tx.Write(record.GetValue()); err != nil {
		return fmt.Errorf("failed to write value bytes: %w", err)
	}

	return nil
}
