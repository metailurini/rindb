package rindb

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// createDummyFile creates a file of the specified size in MB.
func createDummyFile(t *testing.T, dir string, fileName string, sizeMB int) string {
	t.Helper()
	filePath := filepath.Join(dir, fileName)
	sizeBytes := int64(sizeMB) * 1024 * 1024
	file, err := os.Create(filePath)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, file.Close()) }()

	// Seek to the desired size - 1 and write a single byte
	if sizeBytes > 0 {
		_, err = file.Seek(sizeBytes-1, 0)
		assert.NoError(t, err)
		_, err = file.Write([]byte{0})
		assert.NoError(t, err)
	} else {
		// Ensure the file is empty if sizeMB is 0
		err = file.Truncate(0)
		assert.NoError(t, err)
	}

	return filePath
}

func testOptions() []Option {
	return []Option{
		WithDatabaseDir("testdata"),
	}
}

func testConfig() Config {
	return NewConfig(testOptions()...)
}

// TestRindb_Init tests the initialization of the Rindb database.
func TestRindb_Init(t *testing.T) {
	_, err := InitRinDB(testOptions()...)
	assert.NoError(t, err)
}

// TestRindb_Put tests the Put operation of Rindb.
func TestRindb_Put(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	assert.NoError(t, err)
	t.Run("BasicPut", func(t *testing.T) {
		err := rin.Put(Bytes("key"), Bytes("value"))
		assert.NoError(t, err)
	})
	t.Run("Tombstone", func(t *testing.T) {
		err := rin.Put(Bytes("rm-key"), nil)
		assert.NoError(t, err)
	})
}

// TestRindb_Get tests the Get operation of Rindb.
func TestRindb_Get(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig()
	assert.NoError(t, err)
	key := Bytes("key")
	err = rin.Put(key, Bytes("value"))
	assert.NoError(t, err)
	value, err := rin.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value"), value)
}

// TestRindb_Remove tests the Remove operation of Rindb.
func TestRindb_Remove(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig()
	assert.NoError(t, err)
	key := Bytes("rm-key")
	err = rin.Put(key, Bytes("value"))
	assert.NoError(t, err)
	err = rin.Remove(key)
	assert.NoError(t, err)
	value, err := rin.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes(nil), value)
}

// TestRindb_FlushMemtable tests flushing the memtable to an SSTable.
func TestRindb_FlushMemtable(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig()
	assert.NoError(t, err)
	err = rin.Put(Bytes("key"), Bytes("value"))
	assert.NoError(t, err)
	err = rin.Put(Bytes("rm-key"), Bytes("value"))
	assert.NoError(t, err)
	err = rin.Remove(Bytes("rm-key"))
	assert.NoError(t, err)
	ssTableManager, err := InitSSTableManager(rin.config)
	assert.NoError(t, err)
	defer ssTableManager.Close()
	newSSTableFS, err := ssTableManager.NewSSTableFS(0)
	assert.NoError(t, err)
	defer func() { _ = newSSTableFS.Close() }()
	newSStable, err := Flush(rin.config, rin.memtable, newSSTableFS)
	assert.NoError(t, err)
	err = rin.wal.Clean()
	assert.NoError(t, err)
	value, err := newSStable.GetValue(Bytes("rm-key"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes(nil), value)
	value, err = newSStable.GetValue(Bytes("key"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value"), value)
}

// TestRindb_GetPrecedence tests that Get prioritizes Memtable over SSTables.
func TestRindb_GetPrecedence(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig()
	assert.NoError(t, err)
	ssTableManager, err := InitSSTableManager(rin.config)
	assert.NoError(t, err)
	defer ssTableManager.Close()
	fs, err := ssTableManager.NewSSTableFS(0)
	assert.NoError(t, err)
	mem := InitMemtable(rin.config)
	mem.Put(Bytes("k1"), Bytes("v1-sst"))
	_, err = Flush(rin.config, mem, fs)
	assert.NoError(t, err)
	ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
	ssTableManager.levels[0].PushBack(fs)
	err = rin.Put(Bytes("k1"), Bytes("v1-mem"))
	assert.NoError(t, err)
	v, err := rin.Get(Bytes("k1"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1-mem"), v)
}

// TestRindb_ConcurrentCRUD tests concurrent CRUD operations on Rindb.
func TestRindb_ConcurrentCRUD(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig()
	assert.NoError(t, err)
	var wg sync.WaitGroup
	const numGoroutines = 10
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			key := Bytes(fmt.Sprintf("k%d", i))
			value := Bytes(fmt.Sprintf("v%d", i))
			assert.NoError(t, rin.Put(key, value))
			v, err := rin.Get(key)
			assert.NoError(t, err)

			assert.Equal(t, value, v)
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			value = Bytes(fmt.Sprintf("v%d-updated", i))
			assert.NoError(t, rin.Put(key, value))

			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			v, err = rin.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, value, v)
			assert.NoError(t, rin.Remove(key))
		}(i)
	}
	wg.Wait()
}
