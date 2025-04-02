package rindb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// initTempFileSystems creates n temporary FileSystem instances for testing and returns a cleanup function.
func initTempFileSystems(t *testing.T, n int) ([]*FileSystem, func()) {
	fss := make([]*FileSystem, 0, n)
	tempDir := t.TempDir()
	for i := 0; i < n; i++ {
		fs, err := OpenFS(fmt.Sprintf("%s/test-%d", tempDir, i))
		assert.NoError(t, err)
		fss = append(fss, fs)
	}
	return fss, func() {
		for _, fs := range fss {
			_ = fs.Close()
		}
	}
}

// randStringBytes generates a random string of length n as Bytes.
func randStringBytes(n int) Bytes {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

// populateMemtable creates a Memtable and populates it with the given key-value pairs.
func populateMemtable(cfg Config, pairs ...[2]Bytes) Memtable {
	mem := InitMemtable(cfg)
	for _, pair := range pairs {
		mem.Put(pair[0], pair[1])
	}
	return mem
}

// debugSkipList prints the contents of a SkipList for debugging.
func debugSkipList[K Comparable, V any](list *SkipList[K, V]) {
	DEBUG("--header--: %v", list.headNote)
	r := list.headNote.Next()
	for r != nil {
		DEBUG("[%v<>%v] ", r.Key, r.Value)
		for _, v := range r.forwards {
			if v == nil {
				continue
			}
			DEBUG("[%v<>%v] ", v.Key, v.Value)
		}
		fmt.Println()
		r = r.Next()
	}
}
