package diffharness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	rindb "github.com/metailurini/rindb"
	"github.com/stretchr/testify/require"
)

func TestHarnessLogsAndSnapshots(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "db")
	ctx := context.Background()
	db, err := rindb.InitRinDB(ctx, rindb.WithDatabaseDir(dbDir))
	require.NoError(t, err)
	eng := NewRinDBEngine(db)
	logPath := filepath.Join(dir, "log.jsonl")
	h, err := NewHarness(eng, nil, 1, logPath)
	require.NoError(t, err)
	h.Snapshots = append(h.Snapshots, h.Seq)
	t.Cleanup(func() { _ = h.Close(); _ = eng.Close() })

	_, err = h.Step(ctx, PutOp{K: []byte("a"), V: []byte("b")})
	require.NoError(t, err)
	require.Equal(t, uint64(1), h.Seq)

	_, err = h.Step(ctx, SnapOp{})
	require.NoError(t, err)
	require.Len(t, h.Snapshots, 2)
	require.Equal(t, uint64(1), h.Snapshots[1])

	data, err := os.ReadFile(logPath)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 8)

	var first struct {
		I     int    `json:"i"`
		Seq   uint64 `json:"seq"`
		Op    Op     `json:"op"`
		Phase Phase  `json:"phase"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	require.Equal(t, 0, first.I)
	require.Equal(t, uint64(0), first.Seq)
	require.Equal(t, OpPut, first.Op.Kind)
	require.Equal(t, PhasePrepared, first.Phase)
}

// sqliteEngine adapts SQLiteOracle to the Engine interface for testing.
type sqliteEngine struct {
	o   *SQLiteOracle
	seq *uint64
}

func (e *sqliteEngine) Put(ctx context.Context, k, v []byte) error {
	return e.o.PutWithSeq(k, v, *e.seq)
}

func (e *sqliteEngine) Delete(ctx context.Context, k []byte) error {
	return e.o.DelWithSeq(k, *e.seq)
}

func (e *sqliteEngine) Get(ctx context.Context, k []byte, snapshot uint64) ([]byte, bool, error) {
	return e.o.GetWithSeq(k, snapshot)
}

func (e *sqliteEngine) Range(ctx context.Context, lo, hi []byte, snapshot uint64, limit int) ([]KV, error) {
	return e.o.RangeWithSeq(lo, hi, snapshot, limit)
}

func (e *sqliteEngine) NewSnapshot(ctx context.Context) (uint64, error) {
	return e.o.NewSnapshot(ctx)
}

func (e *sqliteEngine) ReleaseSnapshot(ctx context.Context, seq uint64) error {
	return e.o.ReleaseSnapshot(ctx, seq)
}

func (e *sqliteEngine) Close() error { return e.o.Close() }

func (e *sqliteEngine) Begin(ctx context.Context) error    { return e.o.Begin(ctx) }
func (e *sqliteEngine) Commit(ctx context.Context) error   { return e.o.Commit(ctx) }
func (e *sqliteEngine) Rollback(ctx context.Context) error { return e.o.Rollback(ctx) }

func TestHistoricalReads(t *testing.T) {
	dir := t.TempDir()
	ref, err := OpenSQLiteOracle(filepath.Join(dir, "ref.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ref.Close() })

	dbDir := filepath.Join(dir, "db")
	db, err := rindb.InitRinDB(context.Background(), rindb.WithDatabaseDir(dbDir))
	require.NoError(t, err)
	eng := NewRinDBEngine(db)

	logPath := filepath.Join(dir, "log.jsonl")
	h, err := NewHarness(eng, ref, 1, logPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close(); _ = eng.Close() })

	ctx := context.Background()
	h.Snapshots = append(h.Snapshots, h.Seq)
	_, err = h.Step(ctx, PutOp{K: []byte("k"), V: []byte("v1")})
	require.NoError(t, err)
	snap := h.Seq
	_, err = h.Step(ctx, SnapOp{})
	require.NoError(t, err)
	_, err = h.Step(ctx, PutOp{K: []byte("k"), V: []byte("v2")})
	require.NoError(t, err)

	v, ok, err := h.My.Get(ctx, []byte("k"), snap)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("v1"), v)

	v, ok, err = h.My.Get(ctx, []byte("k"), h.Seq)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("v2"), v)
}

func TestRangeHistoricalSnapshot(t *testing.T) {
	dir := t.TempDir()
	ref, err := OpenSQLiteOracle(filepath.Join(dir, "ref.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ref.Close() })

	myOracle, err := OpenSQLiteOracle(filepath.Join(dir, "my.db"))
	require.NoError(t, err)
	eng := &sqliteEngine{o: myOracle}

	logPath := filepath.Join(dir, "log.jsonl")
	h, err := NewHarness(eng, ref, 1, logPath)
	require.NoError(t, err)
	eng.seq = &h.Seq
	t.Cleanup(func() { _ = h.Close(); _ = eng.Close() })

	ctx := context.Background()
	h.Snapshots = append(h.Snapshots, h.Seq)
	_, err = h.Step(ctx, PutOp{K: []byte("a"), V: []byte("1")})
	require.NoError(t, err)
	_, err = h.Step(ctx, PutOp{K: []byte("b"), V: []byte("2")})
	require.NoError(t, err)
	_, err = h.Step(ctx, PutOp{K: []byte("c"), V: []byte("3")})
	require.NoError(t, err)
	snap := h.Seq
	_, err = h.Step(ctx, SnapOp{})
	require.NoError(t, err)

	_, err = h.Step(ctx, PutOp{K: []byte("b"), V: []byte("2'")})
	require.NoError(t, err)
	_, err = h.Step(ctx, DelOp{K: []byte("c")})
	require.NoError(t, err)

	res, err := h.My.Range(ctx, []byte("a"), []byte("z"), snap, 10)
	require.NoError(t, err)
	exp := []KV{{K: []byte("a"), V: []byte("1")}, {K: []byte("b"), V: []byte("2")}, {K: []byte("c"), V: []byte("3")}}
	require.Equal(t, exp, res)
}

func TestHarnessRunChecksInvariants(t *testing.T) {
	dir := t.TempDir()
	ref, err := OpenSQLiteOracle(filepath.Join(dir, "ref.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ref.Close() })

	myOracle, err := OpenSQLiteOracle(filepath.Join(dir, "my.db"))
	require.NoError(t, err)
	eng := &sqliteEngine{o: myOracle}

	logPath := filepath.Join(dir, "log.jsonl")
	h, err := NewHarness(eng, ref, 1, logPath)
	require.NoError(t, err)
	eng.seq = &h.Seq
	t.Cleanup(func() { _ = h.Close(); _ = eng.Close() })

	cfg := Cfg{
		KeyLen:    10,
		ValLenMax: 100,
		RangeMax:  100,
		Weights: map[OpKind]int{
			OpPut:   1,
			OpDel:   1,
			OpGet:   1,
			OpRange: 1,
			OpSnap:  1,
		},
	}
	ctx := context.Background()
	require.NoError(t, h.Run(ctx, cfg, 50))
}

func TestHarnessCrashAndTelemetryHooks(t *testing.T) {
	dir := t.TempDir()
	ref, err := OpenSQLiteOracle(filepath.Join(dir, "ref.db"))
	require.NoError(t, err)
	defer ref.Close()

	myPath := filepath.Join(dir, "my.db")
	myOracle, err := OpenSQLiteOracle(myPath)
	require.NoError(t, err)
	eng := &sqliteEngine{o: myOracle}

	logPath := filepath.Join(dir, "log.jsonl")
	h, err := NewHarness(eng, ref, 1, logPath)
	require.NoError(t, err)
	eng.seq = &h.Seq
	defer h.Close()
	defer eng.Close()

	crashes := 0
	telem := 0
	h.SetCrashHook(func() error {
		crashes++
		if err := eng.Close(); err != nil {
			return err
		}
		myOracle, err = OpenSQLiteOracle(myPath)
		if err != nil {
			return err
		}
		eng.o = myOracle
		return nil
	})
	h.SetTelemetryHook(func(seq uint64, ops int) { telem++ })

	ctx := context.Background()
	h.Snapshots = append(h.Snapshots, h.Seq)
	_, err = h.Step(ctx, PutOp{K: []byte("k"), V: []byte("v")})
	require.NoError(t, err)
	snap := h.Seq
	_, err = h.Step(ctx, SnapOp{})
	require.NoError(t, err)
	require.NoError(t, h.crash())
	v, ok, err := h.My.Get(ctx, []byte("k"), h.Seq)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("v"), v)
	v, ok, err = h.My.Get(ctx, []byte("k"), snap)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("v"), v)

	cfg := Cfg{
		KeyLen:    10,
		ValLenMax: 100,
		RangeMax:  100,
		Weights: map[OpKind]int{
			OpPut:   1,
			OpDel:   1,
			OpGet:   1,
			OpRange: 1,
			OpSnap:  1,
		},
		CrashEvery:     10,
		TelemetryEvery: 5,
	}
	require.NoError(t, h.Run(ctx, cfg, 50))
	require.Greater(t, crashes, 1)
	require.Greater(t, telem, 0)
}

func TestReplayRecoversAfterCrash(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	refPath := filepath.Join(dir, "ref.db")
	ref, err := OpenSQLiteOracle(refPath)
	require.NoError(t, err)
	dbDir := filepath.Join(dir, "db")
	db, err := rindb.InitRinDB(ctx, rindb.WithDatabaseDir(dbDir))
	require.NoError(t, err)
	eng := NewRinDBEngine(db)
	logPath := filepath.Join(dir, "log.jsonl")
	h, err := NewHarness(eng, ref, 1, logPath)
	require.NoError(t, err)
	h.Snapshots = append(h.Snapshots, h.Seq)

	calls := 0
	h.SetCrashHook(func() error {
		calls++
		if calls == 2 {
			_ = eng.Close()
			_ = ref.Close()
			return fmt.Errorf("crash")
		}
		return nil
	})

	_, err = h.Step(ctx, PutOp{K: []byte("k"), V: []byte("v")})
	require.Error(t, err)
	require.Equal(t, 2, calls)
	_ = h.Close()

	db2, err := rindb.InitRinDB(ctx, rindb.WithDatabaseDir(dbDir))
	require.NoError(t, err)
	eng2 := NewRinDBEngine(db2)
	ref2, err := OpenSQLiteOracle(refPath)
	require.NoError(t, err)

	seq, err := Replay(ctx, eng2, ref2, logPath)
	require.NoError(t, err)

	mv, mok, err := eng2.Get(ctx, []byte("k"), seq)
	require.NoError(t, err)
	rv, rok, err := ref2.GetWithSeq([]byte("k"), seq)
	require.NoError(t, err)
	require.Equal(t, mok, rok)
	if mok {
		require.Equal(t, mv, rv)
	}
}

func TestHarnessCloseWithoutSnapshots(t *testing.T) {
	dir := t.TempDir()
	ref, err := OpenSQLiteOracle(filepath.Join(dir, "ref.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ref.Close() })

	my, err := OpenSQLiteOracle(filepath.Join(dir, "my.db"))
	require.NoError(t, err)
	eng := &sqliteEngine{o: my}
	t.Cleanup(func() { _ = eng.Close() })

	logPath := filepath.Join(dir, "log.jsonl")
	h, err := NewHarness(eng, ref, 1, logPath)
	require.NoError(t, err)

	require.NotPanics(t, func() { _ = h.Close() })
}
