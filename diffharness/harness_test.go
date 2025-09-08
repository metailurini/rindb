package diffharness

import (
	"context"
	"encoding/json"
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

	require.NoError(t, h.Step(ctx, Op{Kind: OpPut, K: []byte("a"), V: []byte("b")}))
	require.Equal(t, uint64(1), h.Seq)

	require.NoError(t, h.Step(ctx, Op{Kind: OpSnap}))
	require.Len(t, h.Snapshots, 2)
	require.Equal(t, uint64(1), h.Snapshots[1])

	data, err := os.ReadFile(logPath)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2)

	var first struct {
		I   int    `json:"i"`
		Seq uint64 `json:"seq"`
		Op  Op     `json:"op"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	require.Equal(t, 0, first.I)
	require.Equal(t, uint64(0), first.Seq)
	require.Equal(t, OpPut, first.Op.Kind)
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

func (e *sqliteEngine) Close() error { return e.o.Close() }

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
		KeyLen:    4,
		ValLenMin: 1,
		ValLenMax: 4,
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

	myOracle, err := OpenSQLiteOracle(filepath.Join(dir, "my.db"))
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
	h.SetCrashHook(func() error { crashes++; return nil })
	h.SetTelemetryHook(func(seq uint64, ops int) { telem++ })

	cfg := Cfg{
		KeyLen:    4,
		ValLenMin: 1,
		ValLenMax: 4,
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
	ctx := context.Background()
	require.NoError(t, h.Run(ctx, cfg, 50))
	require.Greater(t, crashes, 0)
	require.Greater(t, telem, 0)
}
