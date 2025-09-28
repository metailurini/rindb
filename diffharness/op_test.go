package diffharness

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type dummyEngine struct {
	putErr error
}

func (d dummyEngine) Begin(context.Context) error               { return nil }
func (d dummyEngine) Commit(context.Context) error              { return nil }
func (d dummyEngine) Rollback(context.Context) error            { return nil }
func (d dummyEngine) Put(context.Context, []byte, []byte) error { return d.putErr }
func (d dummyEngine) Delete(context.Context, []byte) error      { return nil }
func (d dummyEngine) Get(context.Context, []byte, uint64) ([]byte, bool, error) {
	return nil, false, nil
}
func (d dummyEngine) Range(context.Context, []byte, []byte, RangeOrder, uint64, int) ([]KV, error) {
	return nil, nil
}
func (d dummyEngine) NewSnapshot(context.Context) (uint64, error)   { return 0, nil }
func (d dummyEngine) ReleaseSnapshot(context.Context, uint64) error { return nil }
func (d dummyEngine) Close() error                                  { return nil }

func TestPutOp_LoggingAndError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		name       string
		eng        dummyEngine
		wantErr    bool
		wantPhases []Phase
	}{
		{"ok", dummyEngine{}, false, []Phase{PhasePrepared, PhaseMyDone, PhaseRefDone, PhaseCommitted}},
		{"put-fail", dummyEngine{putErr: errors.New("boom")}, true, []Phase{PhasePrepared}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Harness{My: tc.eng}
			var phases []Phase
			logger := PhaseLoggerFunc(func(o Op, seq uint64, p Phase, _ int) error {
				phases = append(phases, p)
				return nil
			})
			op := PutOp{K: []byte("k"), V: []byte("v")}
			_, err := op.Apply(ctx, h, logger)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantPhases, phases)
		})
	}
}

func TestGetOp_Mismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	ref, err := OpenSQLiteOracle(filepath.Join(dir, "ref.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ref.Close() })
	require.NoError(t, ref.Begin(ctx))
	require.NoError(t, ref.PutWithSeq([]byte("k"), []byte("v"), 1))
	require.NoError(t, ref.Commit(ctx))
	h := &Harness{My: dummyEngine{}, Ref: ref}
	logger := PhaseLoggerFunc(func(o Op, seq uint64, p Phase, _ int) error { return nil })
	g := GetOp{K: []byte("k"), SnapSeq: 1}
	_, err = g.Apply(ctx, h, logger)
	var mm *MismatchError
	require.ErrorAs(t, err, &mm)
}

func TestPutOp_DefaultNoCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := &Harness{My: dummyEngine{}}
	logger := PhaseLoggerFunc(func(Op, uint64, Phase, int) error { return nil })
	op := PutOp{start: PhaseCommitted}
	committed, err := op.Apply(ctx, h, logger)
	require.NoError(t, err)
	require.False(t, committed)
}

func TestDelOp_DefaultNoCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := &Harness{My: dummyEngine{}}
	logger := PhaseLoggerFunc(func(Op, uint64, Phase, int) error { return nil })
	op := DelOp{start: PhaseCommitted}
	committed, err := op.Apply(ctx, h, logger)
	require.NoError(t, err)
	require.False(t, committed)
}
