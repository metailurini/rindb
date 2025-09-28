package diffharness

import (
	"context"
	"slices"
	"testing"

	"github.com/metailurini/rindb"
	"github.com/stretchr/testify/require"
)

func TestRinDBEngine_IterRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := rindb.InitRinDB(ctx, rindb.WithDatabaseDir(dir))
	require.NoError(t, err)
	eng := NewRinDBEngine(db)
	t.Cleanup(func() { _ = eng.Close() })

	require.NoError(t, eng.Put(ctx, []byte("a"), []byte("1")))
	require.NoError(t, eng.Put(ctx, []byte("b"), []byte("2")))
	require.NoError(t, eng.Put(ctx, []byte("c"), []byte("3")))

	snap, err := eng.NewSnapshot(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.ReleaseSnapshot(ctx, snap) })

	cases := []struct {
		name  string
		order RangeOrder
		want  [][]byte
	}{{"asc", RangeAsc, [][]byte{[]byte("a"), []byte("b"), []byte("c")}}, {"desc", RangeDesc, [][]byte{[]byte("c"), []byte("b"), []byte("a")}}}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			it, err := eng.IterRange(ctx, []byte("a"), []byte("d"), snap, tc.order)
			require.NoError(t, err)
			defer it.Close()

			var got [][]byte
			for it.HasNext() {
				rec, err := it.Next()
				require.NoError(t, err)
				got = append(got, slices.Clone([]byte(rec.GetKey())))
			}

			require.Equal(t, tc.want, got)
		})
	}
}
