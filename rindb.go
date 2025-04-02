// Package rindb is key-value database
package rindb

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/oklog/ulid/v2"
)

// https://github.com/google/leveldb/blob/main/doc/impl.md

// Rindb is the main database structure
type Rindb struct {
	wal      WAL
	memtable Memtable
	config   Config
	mu       sync.RWMutex
}

// SSTableManager is storage for SSTables
type SSTableManager struct {
	openedFs *list.List
	levels   []*LinkedList[*FileSystem]
	config   Config
	mu       sync.RWMutex
}

func InitSSTableManager(config Config) (*SSTableManager, error) {
	h := &SSTableManager{openedFs: list.New(), config: config}
	err := h.LoadLevels(config.databaseDir)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *SSTableManager) LoadLevels(dir string) error {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	sort.Slice(dirEntries, func(i, j int) bool {
		return dirEntries[i].Name() < dirEntries[j].Name()
	})

	levels := make([]*LinkedList[*FileSystem], 0)
	for _, dirEntry := range dirEntries {
		fileName := dirEntry.Name()
		filePath := path.Join(dir, fileName)
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
	sstableFileName := path.Join(h.config.databaseDir, fmt.Sprintf("l%02d_%s.sst", levelNumb, uid.String()))
	fs, err := OpenFS(sstableFileName)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func (h *SSTableManager) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

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

func (h *SSTableManager) shouldCompact(levelNumb int, level *LinkedList[*FileSystem]) bool {
	const (
		level0Threshold = 4
		baseLevelSizeMB = 10 // MB
	)

	if levelNumb == 0 {
		return level.Len() >= level0Threshold
	}
	totalSize := 0
	iter := level.Iterator()
	for iter.HasNext() {
		fs, _ := iter.Next()
		info, err := os.Stat(fs.filePath)
		if err == nil {
			totalSize += int(info.Size() / (1024 * 1024)) // Convert to MB
		}
	}
	return totalSize >= baseLevelSizeMB*int(math.Pow(10, float64(levelNumb)))
}

func (h *SSTableManager) Compact() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Iterate through each level to check if compaction is needed.
	// We iterate up to the current number of levels. Compaction might create a new level,
	// but the check happens based on the state *before* compaction starts for that iteration.
	for levelNumb := 0; levelNumb < len(h.levels); levelNumb++ {
		// Check if the level exists (it might be nil if levels were skipped during creation)
		if h.levels[levelNumb] == nil {
			// This level doesn't exist, move to the next
			continue
		}
		level := h.levels[levelNumb]

		// *** Integration Point ***
		// Check if this level meets the criteria for compaction.
		if !h.shouldCompact(levelNumb, level) {
			// If the level does not need compaction, skip to the next level.
			INFO("Level %d (size/count: %d) does not meet compaction threshold, skipping.", levelNumb, level.Len()) // Added more info to log
			continue                                                                                                // Move to the next levelNumb
		}

		// If we reach here, the level needs compaction.
		INFO("Level %d (size/count: %d) requires compaction.", levelNumb, level.Len()) // Added more info to log

		// --- Existing Compaction Logic (with potential issues noted in comments) ---

		// TODO: The compaction strategy below needs review.
		// Standard LSM compaction usually involves merging selected files from level N
		// with overlapping files in level N+1, not merging arbitrary batches within level N
		// and pushing them all to N+1. Level 0 compaction is also typically different.

		// For level 0, use the configured compaction threshold for batching
		thresholdFileCount := h.config.level0CompactionThreshold
		if levelNumb > 0 {
			const bufferFileCount = 2
			thresholdFileCount = levelNumb + bufferFileCount
		}
		pickedUpSSTable := make([]SStable, 0, thresholdFileCount)

		/*
			The comment "how can we define and detect threshold properly?"
			is now partially addressed by the `shouldCompact` check above, which decides *if*
			compaction runs. However, the logic *within* the compaction (which files to pick)
			still needs refinement for a proper LSM strategy.
		*/

		levelIterator := level.Iterator()
		// This loop iterates through ALL SSTables in the level if shouldCompact was true.
		// This is likely NOT the desired behavior for levels > 0 in standard LSM.
		for levelIterator.HasNext() {
			// This batching logic seems incorrect for standard LSM.
			// It merges fixed-size batches regardless of key ranges.
			if len(pickedUpSSTable) == thresholdFileCount {
				newLevelNumb := levelNumb + 1
				INFO("Merging batch of %d SSTables from level %d into level %d", len(pickedUpSSTable), levelNumb, newLevelNumb)

				// mergeSSTables removes the source files upon success.
				err := h.mergeSSTables(newLevelNumb, pickedUpSSTable)
				if err != nil {
					// CRITICAL: Error handling needs improvement.
					// If merge fails, the files in pickedUpSSTable have been removed
					// from the level's linked list by PickNext but not physically deleted,
					// and the new merged file might be incomplete or non-existent.
					// The state is inconsistent. Need a recovery mechanism or rollback.
					ERROR("Error merging SSTables during compaction: %v. State may be inconsistent.", err)
					// For now, just return the error, but this is not robust.
					return err
				}

				// Reset the batch
				pickedUpSSTable = make([]SStable, 0, thresholdFileCount)
			}

			// PickNext REMOVES the file system from the linked list *before* merging.
			// This is risky if subsequent operations fail.
			fs, err := levelIterator.PickNext()
			if err != nil {
				ERROR("Error picking next SSTable from level %d iterator: %v", levelNumb, err)
				// State might be inconsistent if some files were already picked.
				return err
			}

			// Open the file system for the SSTable
			if err := fs.Open(); err != nil {
				ERROR("Error opening SSTable file %s for compaction: %v", fs.Path(), err)
				// The file is already removed from the list. Need to handle this.
				// Maybe try to put it back? Or log and continue? Returning is safest for now.
				return err
			}

			// Create the SStable object (reads metadata, bloom filter, index)
			sstable, err := NewSSTable(h.config, fs)
			if err != nil {
				// If NewSSTable fails (e.g., corrupted file), close the FS.
				_ = fs.Close()
				ERROR("Error creating SStable object for %s: %v", fs.Path(), err)
				// File already removed from list. State inconsistent.
				return err
			}

			// Add the successfully opened SStable to the batch.
			pickedUpSSTable = append(pickedUpSSTable, sstable)
		} // End of iterating through SSTables in the level

		// --- Handle any remaining picked up SSTables after the loop finishes ---
		if len(pickedUpSSTable) > 0 {
			newLevelNumb := levelNumb + 1
			INFO("Merging final batch of %d SSTables from level %d into level %d", len(pickedUpSSTable), levelNumb, newLevelNumb)
			err := h.mergeSSTables(newLevelNumb, pickedUpSSTable)
			if err != nil {
				ERROR("Error merging final batch of SSTables during compaction: %v. State may be inconsistent.", err)
				return err
			}
			// No need to reset pickedUpSSTable here.
		}
		// --- End of handling remaining SSTables ---

		// --- Remove the original code block that put files back ---
		/*
			The following block was in the original code. It doesn't make sense to put
			FileSystem objects back into the level list *after* they have been successfully
			merged (mergeSSTables should handle their removal/cleanup). If merging failed,
			a more robust error handling/rollback mechanism is needed than just putting
			the FS pointers back. Removing this block.

			for _, fs := range pickedUpSSTable {
				level.PushBack(fs.FileSystem)
			}
		*/

		// After successfully compacting levelNumb (or deciding not to),
		// the outer loop will increment levelNumb to check the next level.

	} // End of iterating through levels
	return nil
}

// mergeSSTables merges a list of SSTables into a new SSTable at the specified level.
// Assumes the caller holds the necessary lock (e.g., h.mu.Lock()).
func (h *SSTableManager) mergeSSTables(newLevelNumb int, pickedUpSSTable []SStable) error {
	newLevelSSTable, err := h.NewSSTableFS(newLevelNumb)
	if err != nil {
		return err
	}

	if _, err := mergeSSTables(h.config, newLevelSSTable, pickedUpSSTable); err != nil {
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

func (h *SSTableManager) searchKey(key Bytes) (Bytes, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var latestValue Bytes
	var found bool

	// In an LSM-tree, data is organized into levels, where lower levels (e.g., level 0) contain newer data,
	// and higher levels (e.g., level 1, 2, etc.) contain older, compacted data. When searching for a key,
	// we must prioritize the most recent data, as it reflects the latest updates or deletions (e.g., tombstones).
	// Within each level, SSTables are also ordered by recency, especially in level 0 where SSTables may overlap.
	// Iterating from the bottom (most recent SSTable) to the top (oldest SSTable) ensures we find the latest
	// version of the key first. Once a key is found in a level, we can stop searching that level, as newer
	// levels take precedence, and within a level, the most recent SSTable's value is authoritative.
	// This bottom-up traversal aligns with LSM's principle of prioritizing recency in a write-heavy system.
	for levelNumb := range h.levels {
		if h.levels[levelNumb] == nil {
			// No more levels to search
			break
		}

		iterator := h.levels[levelNumb].IteratorFromBottom()
		fs := iterator.Value()
		for {
			if err := fs.Open(); err != nil {
				return nil, err
			}
			sstable, err := NewSSTable(h.config, fs)
			if err != nil {
				_ = fs.Close()
				return nil, err
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

			if !iterator.HasPrev() {
				break
			}

			fs, err = iterator.Prev()
			if err != nil {
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

func mergeSSTables(config Config, target *FileSystem, sources []SStable) (SStable, error) {
	memtable := InitMemtable(config)
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
			memtable.Put(record.GetKey(), record.GetValue())
		}
	}
	sstable, err := Flush(config, memtable, target)
	if err != nil {
		return SStable{}, err
	}
	return sstable, nil
}

func InitRinDB(opts ...Option) (Rindb, error) {
	cfg := NewConfig(opts...)
	walPath := path.Join(cfg.databaseDir, "WAL")
	fs, err := OpenFS(walPath)
	if err != nil {
		return Rindb{}, err
	}
	wal := NewWAL(cfg, fs)
	memtable, err := wal.Load()
	if err != nil {
		return Rindb{}, err
	}
	return Rindb{
		wal:      wal,
		memtable: memtable,
		config:   cfg,
	}, nil
}

func (r *Rindb) Get(key Bytes) (Bytes, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	value, err := r.memtable.Get(key)
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}
	// Key not in memtable, check SSTables
	ssTableManager, err := InitSSTableManager(r.config)
	if err != nil {
		return nil, err
	}
	defer ssTableManager.Close()
	return ssTableManager.searchKey(key)
}

const maxMemtableSize = 1000

func (r *Rindb) Put(key, value Bytes) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	record := RecordImpl{Key: key, Value: value}
	if err := r.wal.Append(record); err != nil {
		return err
	}
	r.memtable.Put(key, value)

	// Check size and flush if needed
	if r.memtable.data.Len() >= r.config.maxMemtableSize {
		ssTableManager, err := InitSSTableManager(r.config)
		if err != nil {
			return err
		}
		defer ssTableManager.Close()
		if ssTableManager.levels[0] != nil && ssTableManager.levels[0].Len() > r.config.level0CompactionThreshold {
			if err := ssTableManager.Compact(); err != nil {
				return err
			}
		}

		fs, err := ssTableManager.NewSSTableFS(0)
		if err != nil {
			return err
		}
		_, err = Flush(r.config, r.memtable, fs)
		if err != nil {
			return err
		}
		fs.Close()
		r.memtable.Clear()
		if err := r.wal.Clean(); err != nil {
			return err
		}
	}
	return nil
}

func (r *Rindb) Remove(key Bytes) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	record := RecordImpl{Key: key, Value: nil}
	if err := r.wal.Append(record); err != nil {
		return err
	}
	r.memtable.Put(record.GetKey(), record.GetValue())
	return nil
}
