package diffharness

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLiteOracle_BasicOperations(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "oracle.db")
	o, err := OpenSQLiteOracle(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, o.Close()) })

	require.NoError(t, o.PutWithSeq([]byte("a"), []byte("va"), 1))
	v, ok, err := o.GetWithSeq([]byte("a"), 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("va"), v)

	require.NoError(t, o.DelWithSeq([]byte("a"), 2))
	v, ok, err = o.GetWithSeq([]byte("a"), 2)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, v)

	require.NoError(t, o.PutWithSeq([]byte("a"), []byte("va2"), 3))
	require.NoError(t, o.PutWithSeq([]byte("b"), []byte("vb"), 4))

	res, err := o.RangeWithSeq([]byte("a"), []byte("z"), 4, 10)
	require.NoError(t, err)
	require.Len(t, res, 2)
	require.Equal(t, []byte("a"), res[0].K)
	require.Equal(t, []byte("va2"), res[0].V)
	require.Equal(t, []byte("b"), res[1].K)
	require.Equal(t, []byte("vb"), res[1].V)

	res, err = o.RangeWithSeq([]byte("a"), []byte("z"), 2, 10)
	require.NoError(t, err)
	require.Len(t, res, 0)

	res, err = o.RangeWithSeq([]byte("a"), []byte("z"), 4, 1)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, []byte("a"), res[0].K)
}
