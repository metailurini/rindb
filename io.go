package rindb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var byteOrder = binary.LittleEndian

func write(storage io.Writer, key, value Bytes) error {
	keyLenBytes := [8]byte{}
	valueLenBytes := [8]byte{}

	byteOrder.PutUint64(keyLenBytes[:], uint64(len(key)))
	byteOrder.PutUint64(valueLenBytes[:], uint64(len(value)))

	if _, err := storage.Write(keyLenBytes[:]); err != nil {
		return fmt.Errorf("failed to write key length: %w", err)
	}

	if _, err := storage.Write(valueLenBytes[:]); err != nil {
		return fmt.Errorf("failed to write value length: %w", err)
	}

	if err := binary.Write(storage, byteOrder, key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	if err := binary.Write(storage, byteOrder, value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	return nil
}

func read(storage io.Reader) (key, value Bytes, err error) {
	keyLenBytes := [8]byte{}
	if _, err := storage.Read(keyLenBytes[:]); err != nil {
		return nil, nil, fmt.Errorf("failed to read key length: %w", err)
	}

	valueLenBytes := [8]byte{}
	if _, err := storage.Read(valueLenBytes[:]); err != nil {
		return nil, nil, fmt.Errorf("failed to read value length: %w", err)
	}

	keyLen := byteOrder.Uint64(keyLenBytes[:])
	valueLen := byteOrder.Uint64(valueLenBytes[:])

	keyBytes := bytes.NewBuffer(Bytes{})

	const defaultReadStep = uint64(255)

	for keyLen > 0 {
		step := defaultReadStep
		if keyLen < step {
			step = keyLen
		}
		keyLen -= step

		tempBytes := make(Bytes, step)

		if err := binary.Read(storage, byteOrder, tempBytes); err != nil {
			return nil, nil, fmt.Errorf("failed to read key: %w", err)
		}

		if _, err := keyBytes.Write(tempBytes); err != nil {
			return nil, nil, fmt.Errorf("failed to write key: %w", err)
		}
	}

	valueBytes := bytes.NewBuffer(Bytes{})
	for valueLen > 0 {
		step := defaultReadStep
		if valueLen < step {
			step = valueLen
		}
		valueLen -= step

		tempBytes := make(Bytes, step)

		if err := binary.Read(storage, byteOrder, tempBytes); err != nil {
			return nil, nil, fmt.Errorf("failed to read value: %w", err)
		}

		if _, err := valueBytes.Write(tempBytes); err != nil {
			return nil, nil, fmt.Errorf("failed to write value: %w", err)
		}
	}

	key = keyBytes.Bytes()
	value = valueBytes.Bytes()

	return
}
