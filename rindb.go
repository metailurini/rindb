// Package rindb is key-value database
package rindb

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/oklog/ulid/v2"
)

// https://github.com/google/leveldb/blob/main/doc/impl.md

var (
	dbDirectory = "testdata"
	walName     = "WAL"
)

// Rindb is the main database structure
type Rindb struct {
	wal      WAL
	memtable Memtable
}

// SSTableManager is storage for SSTables
type SSTableManager struct {
	openedFs *list.List
	levels   []*LinkedList[*FileSystem]
}

func InitSSTableManager() (*SSTableManager, error) {
	h := &SSTableManager{openedFs: list.New()}
	err := h.LoadLevels()
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *SSTableManager) LoadLevels() error {
	dirEntries, err := os.ReadDir(dbDirectory)
	if err != nil {
		return err
	}

	sort.Slice(dirEntries, func(i, j int) bool {
		return dirEntries[i].Name() < dirEntries[j].Name()
	})

	levels := make([]*LinkedList[*FileSystem], 0)
	for _, dirEntry := range dirEntries {
		fileName := dirEntry.Name()
		filePath := path.Join(dbDirectory, fileName)
		isSSTable := strings.HasSuffix(filePath, ".sst")
		if !isSSTable {
			continue
		}

		idx := strings.Index(fileName, "_")
		if idx == -1 {
			ERROR("Invalid sstable name: %s", fileName)
			continue
		}

		levelNumb, err := strconv.ParseInt(fileName[1:idx], 32, 32)
		if err != nil {
			return err
		}

		extLevelNumb := int(levelNumb) + 1
		if extLevelNumb > len(levels) {
			levels = append(levels, make([]*LinkedList[*FileSystem], extLevelNumb-len(levels))...)
		}

		if levels[levelNumb] == nil {
			levels[levelNumb] = InitLinkedList[*FileSystem]()
		}
		levels[levelNumb].PushBack(&FileSystem{filePath: filePath})
	}
	h.levels = levels
	return nil
}

func (h *SSTableManager) NewSSTableFS(levelNumb int) (*FileSystem, error) {
	uid := ulid.Make()
	sstableFileName := path.Join(dbDirectory, fmt.Sprintf("l%02d_%s.sst", levelNumb, uid.String()))
	fs, err := OpenFS(sstableFileName)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func (h *SSTableManager) Close() {
	element := h.openedFs.Front()
	for element != nil {
		fs, ok := element.Value.(*FileSystem)
		if !ok {
			ERROR("can not cast element to file system")
			break
		}

		if err := fs.Close(); err != nil {
			ERROR("Error closing file %s: %v", fs.Path(), err)
		}
		INFO("Closed %s successfully", fs.Path())
		element = element.Next()
	}

	for _, level := range h.levels {
		levelIterator := level.Iterator()
		for levelIterator.HasNext() {
			fs, err := levelIterator.Next()
			if err != nil {
				ERROR("Error iterating through level: %v", err)
				continue
			}

			if !fs.IsOpened() {
				INFO("File %s is already closed", fs.Path())
				continue
			}

			if err := fs.Close(); err != nil {
				ERROR("Error closing file %s: %v", fs.Path(), err)
				continue
			}
			INFO("Closed %s successfully", fs.Path())
		}
	}
}

/*
TODO:
train of thought:

	  get value by key:
	    if it was found in memtable -> return
		else:
		  for level in all levels:
		    check value if it's in that level (using bloom filter)
			  not -> continue
		    for sstable in level.sstables:
			  check and value by key

SSTableManager:
level:

	[]*LinkedList[]

define structure levels

	|

(upgrade)

	|
	V

list files

	|

(upgrade)

	|
	V

list files + bloom filter file (l0_bl)

	|

(upgrade)

	|
	V

MANIFEST:
  - level 0: file 1, file 2,  ...
    bloom filter: <bin>
  - level n1: file 1, file 2,  ...  bloom filter: <bin>
*/
func (h *SSTableManager) Compact() error {
	levelNumb := 0
	for levelNumb != len(h.levels) {
		level := h.levels[levelNumb]

		const bufferFileCount = 2
		thresholdFileCount := levelNumb + bufferFileCount
		pickedUpSSTable := make([]SStable, 0, thresholdFileCount)

		/*
			how can we define and detect threshold properly?
		*/

		levelIterator := level.Iterator()
		for levelIterator.HasNext() {
			if len(pickedUpSSTable) == thresholdFileCount {
				newLevelNumb := levelNumb + 1

				err := h.mergeSSTables(newLevelNumb, pickedUpSSTable)
				if err != nil {
					return err
				}

				pickedUpSSTable = make([]SStable, 0)
			}

			fs, err := levelIterator.PickNext()
			if err != nil {
				return err
			}

			if err := fs.Open(); err != nil {
				return err
			}

			sstable, err := NewSSTable(fs)
			if err != nil {
				return err
			}

			pickedUpSSTable = append(pickedUpSSTable, sstable)
		}

		for _, fs := range pickedUpSSTable {
			level.PushBack(fs.FileSystem)
		}
		levelNumb += 1
	}
	return nil
}

func (h *SSTableManager) mergeSSTables(newLevelNumb int, pickedUpSSTable []SStable) error {
	newLevelSSTable, err := h.NewSSTableFS(newLevelNumb)
	if err != nil {
		return err
	}

	if _, err := mergeSSTables(newLevelSSTable, pickedUpSSTable); err != nil {
		return err
	}

	if len(h.levels) == newLevelNumb {
		h.levels = append(h.levels, InitLinkedList[*FileSystem]())
	}
	h.levels[newLevelNumb].PushBack(newLevelSSTable)

	// remove merged sstable
	for _, sstable := range pickedUpSSTable {
		if err := os.Remove(sstable.Path()); err != nil {
			log.Printf("Error removing file %s: %v", sstable.Path(), err)
		}
	}
	return nil
}

/*
TODO:
-> to search key, so in the level we have to move backward
-> implement double ll remove the current single ll
-> so implement single -> double should be compatible, copy concept of list in built-in package
-> implement full tests for the linked list
*/
func (h *SSTableManager) searchKey(key Bytes) (Bytes, error) {
	var latestValue Bytes
	var found bool

	for i := range h.levels {
		if h.levels[i] == nil {
			// No more levels to search
			break
		}
		iterator := h.levels[i].Iterator()
		for iterator.HasNext() {
			fs, err := iterator.Next()
			if err != nil {
				return nil, err
			}
			if err := fs.Open(); err != nil {
				return nil, err
			}
			sstable, err := NewSSTable(fs)
			if err != nil {
				_ = fs.Close()
				return nil, err
			}

			if !sstable.Bloom.Lookup(key) {
				_ = fs.Close()
				continue
			}

			value, err := sstable.GetValue(key)
			if err == nil {
				// Found a value or tombstone; this is the latest so far
				latestValue = value
				found = true
				_ = fs.Close()
				break
			}
			if !errors.Is(err, ErrKeyNotFound) {
				_ = fs.Close()
				return nil, err
			}
			if err := fs.Close(); err != nil {
				return nil, err
			}
		}
		if found {
			break
		}
	}

	if found {
		return latestValue, nil
	}
	return nil, ErrKeyNotFound
}

func mergeSSTables(target *FileSystem, sources []SStable) (SStable, error) {
	memtable := InitMemtable()
	for _, sstable := range sources {
		iterator, err := sstable.Iterator()
		if err != nil {
			return SStable{}, err
		}
		for iterator.HasNext() {
			record, err := iterator.Next()
			if err != nil {
				return SStable{}, err
			}
			// TODO: add logic/test ignore deleted record
			memtable.Put(record.GetKey(), record.GetValue())
		}
	}
	sstable, err := Flush(memtable, target)
	if err != nil {
		return SStable{}, err
	}
	return sstable, nil
}

func InitRinDB() (Rindb, error) {
	walPath := path.Join(dbDirectory, walName)
	fs, err := OpenFS(walPath)
	if err != nil {
		return Rindb{}, err
	}

	wal := NewWAL(fs)
	memtable, err := wal.Load()
	if err != nil {
		return Rindb{}, err
	}
	return Rindb{
		wal:      wal,
		memtable: memtable,
	}, nil
}

func (r Rindb) Get(key Bytes) (Bytes, error) {
	value, err := r.memtable.Get(key)
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}
	// Key not in memtable, check SSTables
	ssTableManager, err := InitSSTableManager()
	if err != nil {
		return nil, err
	}
	defer ssTableManager.Close()
	return ssTableManager.searchKey(key)
}

const maxMemtableSize = 1000

func (r Rindb) Put(key, value Bytes) error {
	record := RecordImpl{Key: key, Value: value}
	if err := r.wal.Append(record); err != nil {
		return err
	}
	r.memtable.Put(key, value)

	// Check size and flush if needed
	if r.memtable.data.Len() >= maxMemtableSize {
		ssTableManager, err := InitSSTableManager()
		if err != nil {
			return err
		}
		defer ssTableManager.Close()
		if ssTableManager.levels[0] != nil && ssTableManager.levels[0].Len() > 2 { // Simple threshold
			if err := ssTableManager.Compact(); err != nil {
				return err
			}
		}

		fs, err := ssTableManager.NewSSTableFS(0)
		if err != nil {
			return err
		}
		_, err = Flush(r.memtable, fs)
		if err != nil {
			return err
		}
		if err := r.wal.Clean(); err != nil {
			return err
		}
		fs.Close()
	}
	return nil
}

func (r Rindb) Remove(key Bytes) error {
	record := RecordImpl{Key: key, Value: nil}
	if err := r.wal.Append(record); err != nil {
		return err
	}
	r.memtable.Put(record.GetKey(), record.GetValue())
	return nil
}
