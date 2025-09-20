package diffharness

import (
	"context"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/metailurini/rindb"
	"github.com/stretchr/testify/require"
)

func TestCheckRangeIterNextPrev(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	ref, err := OpenSQLiteOracle(filepath.Join(dir, "ref.db"))
	require.NoError(t, err)
	db, err := rindb.InitRinDB(ctx, rindb.WithDatabaseDir(filepath.Join(dir, "db")))
	require.NoError(t, err)
	eng := NewRinDBEngine(db)
	logPath := filepath.Join(dir, "log.jsonl")
	h, err := NewHarness(eng, ref, 1, logPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close(); _ = eng.Close(); _ = ref.Close() })

	r := rand.New(rand.NewSource(1))
	for i := 0; i < 5; i++ {
		k := randKey(r, 8)
		v := randValue(r, 8)
		_, err = h.Step(ctx, PutOp{K: k, V: v})
		require.NoError(t, err)
	}

	cfg := Cfg{KeyLen: 8, RangeMax: 10, IterWalk: 10}
	r2 := rand.New(rand.NewSource(2))
	require.NoError(t, checkRangeIterNextPrev(ctx, h, r2, cfg))
}
