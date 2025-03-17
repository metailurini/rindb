package rindb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func debugList[K Comparable, V any](list *SkipList[K, V]) {
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

func randStringBytes(i int) Bytes {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, i)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}
