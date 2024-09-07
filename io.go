package rindb

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
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
		return nil, errors.Wrap(err, "failed to read key length")
	}

	valueLen, err := ReadNumber(storage)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read value length")
	}

	keyBytes := bytes.NewBuffer(nil)

	const defaultReadStep = uint64(255)

	for keyLen > 0 {
		step := defaultReadStep
		if keyLen < step {
			step = keyLen
		}
		keyLen -= step

		tempBytes := make(Bytes, step)

		if err := binary.Read(storage, byteOrder, tempBytes); err != nil {
			return nil, errors.Wrap(err, "failed to read key: %w")
		}

		if _, err := keyBytes.Write(tempBytes); err != nil {
			return nil, errors.Wrap(err, "failed to write key: %w")
		}
	}

	valueBytes := bytes.NewBuffer(nil)
	for valueLen > 0 {
		step := defaultReadStep
		if valueLen < step {
			step = valueLen
		}
		valueLen -= step

		tempBytes := make(Bytes, step)

		if err := binary.Read(storage, byteOrder, tempBytes); err != nil {
			return nil, errors.Wrap(err, "failed to read value: %w")
		}

		if _, err := valueBytes.Write(tempBytes); err != nil {
			return nil, errors.Wrap(err, "failed to write value: %w")
		}
	}

	return RecordImpl{
		Key:   keyBytes.Bytes(),
		Value: valueBytes.Bytes(),
	}, nil
}

func WriteNumber(storage io.Writer, number uint64) error {
	numBytes := [mdByteSize]byte{}
	byteOrder.PutUint64(numBytes[:], number)
	if _, err := storage.Write(numBytes[:]); err != nil {
		return err
	}
	return nil
}

func WriteRecord(storage io.Writer, record Record) error {
	if err := WriteNumber(storage, uint64(len(record.GetKey()))); err != nil {
		return errors.Wrap(err, "failed to write key length")
	}

	if err := WriteNumber(storage, uint64(len(record.GetValue()))); err != nil {
		return errors.Wrap(err, "failed to write value length")
	}

	if err := binary.Write(storage, byteOrder, record.GetKey()); err != nil {
		return errors.Wrap(err, "failed to write key: %w")
	}

	if err := binary.Write(storage, byteOrder, record.GetValue()); err != nil {
		return errors.Wrap(err, "failed to write value: %w")
	}

	return nil
}
