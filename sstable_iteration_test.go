package rindb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableIteratorReverse(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 3))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.Iterator()
	require.NoError(t, err)

	var fwd []Bytes
	for it.HasNext() {
		rec, err := it.Next()
		require.NoError(t, err)
		fwd = append(fwd, rec.GetKey())
	}

	var rev []Bytes
	for it.HasPrev() {
		rec, err := it.Prev()
		require.NoError(t, err)
		rev = append(rev, rec.GetKey())
	}

	assert.Equal(t, []Bytes{Bytes("a"), Bytes("b"), Bytes("c")}, fwd)
	assert.Equal(t, []Bytes{Bytes("c"), Bytes("b"), Bytes("a")}, rev)
}

func TestSSTableIteratorMixed(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.Iterator()
	require.NoError(t, err)

	assert.True(t, it.HasNext())
	rec, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	assert.True(t, it.HasPrev())
	rec, err = it.Prev()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	assert.True(t, it.HasNext())
	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	assert.True(t, it.HasNext())
	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())
}

func TestSSTableIRangeReverse(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 3))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.IRange(Bytes("a"), Bytes("c"))
	require.NoError(t, err)

	var fwd []Bytes
	for it.HasNext() {
		rec, err := it.Next()
		require.NoError(t, err)
		fwd = append(fwd, rec.GetKey())
	}

	var rev []Bytes
	for it.HasPrev() {
		rec, err := it.Prev()
		require.NoError(t, err)
		rev = append(rev, rec.GetKey())
	}

	assert.Equal(t, []Bytes{Bytes("a"), Bytes("b"), Bytes("c")}, fwd)
	assert.Equal(t, []Bytes{Bytes("c"), Bytes("b"), Bytes("a")}, rev)
}

type historyIterFunc func(SStable) (Iterator[Record], *offsetStack, error)

func testHistoryLimit(t *testing.T, mk historyIterFunc) {
	t.Helper()
	cases := []struct {
		name    string
		history int
		keys    []string
	}{
		{"h0", 0, []string{"a", "b", "c", "d", "e"}},
		{"h1", 1, []string{"a", "b", "c", "d", "e"}},
		{"h2", 2, []string{"a", "b", "c", "d", "e"}},
		{"empty_keys_h1", 1, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.sstableIterMaxHistory = tc.history
			ctx := context.Background()
			fss, closer := initTempFileSystems(t, 1, nil)
			defer closer()
			fs := fss[0]

			mem := InitMemtable(cfg)
			for i, k := range tc.keys {
				mem.Put(newRecord(Bytes(k), Bytes("v"), uint64(i+1)))
			}

			if len(tc.keys) == 0 {
				offs := newOffsetStack(tc.history)
				assert.Equal(t, 0, offs.len())
				return
			}

			sst, _, err := flush(ctx, cfg, mem, fs)
			require.NoError(t, err)

			it, offs, err := mk(sst)
			require.NoError(t, err)

			for range tc.keys {
				_, err := it.Next()
				require.NoError(t, err)
			}

			expLen := tc.history
			if expLen > len(tc.keys) {
				expLen = len(tc.keys)
			}
			assert.Equal(t, expLen, offs.len())

			for i := 0; i < tc.history && i < len(tc.keys); i++ {
				rec, err := it.Prev()
				require.NoError(t, err)
				assert.Equal(t, Bytes(tc.keys[len(tc.keys)-1-i]), rec.GetKey())
			}

			_, err = it.Prev()
			assert.ErrorIs(t, err, EOI)
		})
	}
}

func TestSSTableIteratorHistoryLimit(t *testing.T) {
	testHistoryLimit(t, func(sst SStable) (Iterator[Record], *offsetStack, error) {
		it, err := sst.Iterator()
		if err != nil {
			return nil, nil, err
		}
		return it, it.(*sstableIterator).offs, nil
	})
}

func TestSSTableIRangeHistoryLimit(t *testing.T) {
	testHistoryLimit(t, func(sst SStable) (Iterator[Record], *offsetStack, error) {
		it, err := sst.IRange(Bytes("a"), Bytes("e"))
		if err != nil {
			return nil, nil, err
		}
		return it, it.(*sstableIRange).offs, nil
	})
}

func TestSSTableIteratorHistoryReset(t *testing.T) {
	cfg := testConfig()
	cfg.sstableIterMaxHistory = 2
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("v"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("v"), 2))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.Iterator()
	require.NoError(t, err)

	si := it.(*sstableIterator)
	_, err = it.Next()
	require.NoError(t, err)
	_, err = it.Next()
	require.NoError(t, err)
	require.True(t, it.HasPrev())

	si.offset = 0
	si.offs = newOffsetStack(cfg.sstableIterMaxHistory)

	assert.False(t, it.HasPrev())
	assert.Equal(t, 0, si.offs.len())
}
