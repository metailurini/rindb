package rindb

import (
	"os"
	"sync"
)

var (
	mapFileMu   sync.RWMutex
	mapFileFunc = mapFile
)

func callMapFile(f *os.File) (*mmapHandle, error) {
	mapFileMu.RLock()
	fn := mapFileFunc
	mapFileMu.RUnlock()
	return fn(f)
}

func withMapFileStub(fn func(*os.File) (*mmapHandle, error)) func() {
	mapFileMu.Lock()
	prev := mapFileFunc
	mapFileFunc = fn
	mapFileMu.Unlock()
	return func() {
		mapFileMu.Lock()
		mapFileFunc = prev
		mapFileMu.Unlock()
	}
}
