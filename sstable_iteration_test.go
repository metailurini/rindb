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
	}{
		{"h0", 0},
		{"h1", 1},
		{"h2", 2},
	}
	keys := []string{"a", "b", "c", "d", "e"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.sstableIterMaxHistory = tc.history
			ctx := context.Background()
			fss, closer := initTempFileSystems(t, 1, nil)
			defer closer()
			fs := fss[0]

			mem := InitMemtable(cfg)
			for i, k := range keys {
				mem.Put(newRecord(Bytes(k), Bytes("v"), uint64(i+1)))
			}

			sst, _, err := flush(ctx, cfg, mem, fs)
			require.NoError(t, err)

			it, offs, err := mk(sst)
			require.NoError(t, err)

			for range keys {
				_, err := it.Next()
				require.NoError(t, err)
			}

			assert.Equal(t, tc.history, len(offs.buf))
			expLen := tc.history
			if expLen > len(keys) {
				expLen = len(keys)
			}
			assert.Equal(t, expLen, offs.len())

			for i := 0; i < tc.history && i < len(keys); i++ {
				rec, err := it.Prev()
				require.NoError(t, err)
				assert.Equal(t, Bytes(keys[len(keys)-1-i]), rec.GetKey())
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
