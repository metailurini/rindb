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
