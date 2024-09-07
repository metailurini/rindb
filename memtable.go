package rindb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
)

var _ CmpType = (*Bytes)(nil)

type Bytes []byte

func (b Bytes) Compare(other any) int {
	o, _ := other.(Bytes)
	return bytes.Compare(b, o)
}

type memtable struct {
	data *skipList[Bytes, Bytes]
}

func initMemtable() *memtable {
	list, _ := initSkipList[Bytes, Bytes]()
	return &memtable{data: list}
}

func (m *memtable) get(key Bytes) (Bytes, error) {
	return m.data.get(key)
}

func (m *memtable) put(key, value Bytes) {
	m.data.put(key, value)
}

func (m *memtable) flush(diskPath string) error {
	file, err := os.OpenFile(filepath.Clean(diskPath), os.O_RDWR|os.O_CREATE, fileSystemPermission)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}()

	node := m.data.head()
	for {
		node = node.next()
		if node == nil {
			break
		}
		err := write(file, node.key, node.value)
		if err != nil {
			return err
		}
	}

	err = file.Sync()
	if err != nil {
		return err
	}

	return nil
}
