package rindb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestShardGetEntryLockedRemovesClosedEntry covers the branch where a closed entry is pruned.
func TestShardGetEntryLockedRemovesClosedEntry(t *testing.T) {
	parent := &tableCache{}
	s := &shard{
		items:  make(map[tableKey]*entry),
		parent: parent,
	}
	key := tableKey{FileNum: 1}
	tcEntry := &tableCacheEntry{
		Table:       &SStable{},
		actualBytes: 1,
		closer:      func(*SStable) error { return nil },
	}
	tcEntry.closed.Store(true)

	e := &entry{key: key, entry: tcEntry, seg: segProbation}
	e.elem = s.prob.PushFront(e)
	s.items[key] = e
	s.probBytes = tcEntry.actualBytes
	s.usedBytes = tcEntry.actualBytes
	parent.totalBytes.Add(tcEntry.actualBytes)

	s.mu.Lock()
	got, err := s.getEntryLocked(key)
	s.mu.Unlock()
	require.NoError(t, err)
	require.Nil(t, got)
	require.Zero(t, s.prob.Len())
	require.Zero(t, s.probBytes)
	require.Zero(t, s.usedBytes)
	require.EqualValues(t, 0, parent.totalBytes.Load())
	_, ok := s.items[key]
	require.False(t, ok)
}

// TestPromoteOnHitDemoteAndBreak ensures demotion and the break path when protected list is empty.
func TestPromoteOnHitDemoteAndBreak(t *testing.T) {
	s := &shard{
		items:        make(map[tableKey]*entry),
		protCapBytes: 0,
		protBytes:    1, // inconsistent accounting to trigger break path
	}
	key := tableKey{FileNum: 1}
	e := &entry{
		key:   key,
		entry: &tableCacheEntry{actualBytes: 1},
		seg:   segProbation,
	}
	e.elem = s.prob.PushFront(e)
	s.items[key] = e
	s.probBytes = e.entry.actualBytes
	s.usedBytes = s.probBytes + s.protBytes

	s.promoteOnHit(context.Background(), e)

	require.Equal(t, segProbation, e.seg)
	require.Equal(t, 1, s.prob.Len())
	require.Equal(t, 0, s.prot.Len())
	require.EqualValues(t, 1, s.protBytes)
}

// TestChooseVictimFromProtectedSkipsPinned covers iteration over protected list.
func TestChooseVictimFromProtectedSkipsPinned(t *testing.T) {
	s := &shard{}
	eUnpinned := &entry{key: tableKey{FileNum: 1}, entry: &tableCacheEntry{}, seg: segProtected}
	eUnpinned.elem = s.prot.PushBack(eUnpinned)
	ePinned := &entry{key: tableKey{FileNum: 2}, entry: &tableCacheEntry{}, seg: segProtected, pinned: true}
	ePinned.elem = s.prot.PushBack(ePinned) // pinned at tail

	victim := s.chooseVictim()
	require.Equal(t, eUnpinned, victim)
}

// TestEvictOrDemoteLockedDemotesTail verifies demotion when protected exceeds cap.
func TestEvictOrDemoteLockedDemotesTail(t *testing.T) {
	parent := &tableCache{}
	s := &shard{
		items:        make(map[tableKey]*entry),
		protCapBytes: 1,
		capBytes:     10,
		parent:       parent,
	}
	e1 := &entry{key: tableKey{FileNum: 1}, entry: &tableCacheEntry{actualBytes: 1}, seg: segProtected}
	e1.elem = s.prot.PushFront(e1)
	e2 := &entry{key: tableKey{FileNum: 2}, entry: &tableCacheEntry{actualBytes: 1}, seg: segProtected}
	e2.elem = s.prot.PushBack(e2)
	s.items[e1.key] = e1
	s.items[e2.key] = e2
	s.protBytes = e1.entry.actualBytes + e2.entry.actualBytes
	s.usedBytes = s.protBytes

	s.mu.Lock()
	ok := s.evictOrDemoteLocked(context.Background(), nil)
	s.mu.Unlock()

	require.True(t, ok)
	require.Equal(t, segProtected, e1.seg)
	require.Equal(t, segProbation, e2.seg)
	require.Equal(t, 1, s.prot.Len())
	require.Equal(t, 1, s.prob.Len())
	require.EqualValues(t, e1.entry.actualBytes, s.protBytes)
	require.EqualValues(t, e2.entry.actualBytes, s.probBytes)
}

// TestEvictOrDemoteLockedBreakOnEmptyProtected exercises the break when protected list is empty.
func TestEvictOrDemoteLockedBreakOnEmptyProtected(t *testing.T) {
	s := &shard{
		protCapBytes: 0,
		protBytes:    1,
		capBytes:     10,
	}

	s.mu.Lock()
	ok := s.evictOrDemoteLocked(context.Background(), nil)
	s.mu.Unlock()

	require.True(t, ok)
	require.EqualValues(t, 1, s.protBytes)
	require.Equal(t, 0, s.prot.Len())
}

// TestEvictOrDemoteLockedDropsRecentWhenNoVictim ensures recent entry is dropped when all are pinned.
func TestEvictOrDemoteLockedDropsRecentWhenNoVictim(t *testing.T) {
	parent := &tableCache{}
	recentEntry := &tableCacheEntry{
		Table:       &SStable{},
		actualBytes: 2,
		closer:      func(*SStable) error { return nil },
	}
	recent := &entry{
		key:    tableKey{FileNum: 1},
		entry:  recentEntry,
		seg:    segProbation,
		pinned: true,
	}

	s := &shard{
		items:    map[tableKey]*entry{recent.key: recent},
		capBytes: 1,
		parent:   parent,
	}
	recent.elem = s.prob.PushFront(recent)
	s.probBytes = recentEntry.actualBytes
	s.usedBytes = recentEntry.actualBytes
	parent.totalBytes.Add(recentEntry.actualBytes)

	s.mu.Lock()
	ok := s.evictOrDemoteLocked(context.Background(), recent)
	s.mu.Unlock()

	require.False(t, ok)
	require.Equal(t, 0, s.prob.Len())
	_, exists := s.items[recent.key]
	require.False(t, exists)
	require.EqualValues(t, 0, s.usedBytes)
	require.EqualValues(t, 0, parent.totalBytes.Load())
}
