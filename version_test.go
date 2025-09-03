package rindb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionEdit(t *testing.T) {
	t.Run("ComparatorMismatch", func(t *testing.T) {
		vs := &VersionSet{Comparator: "bytes"}
		edit := VersionEdit{ComparatorName: "rev"}
		err := edit.Apply(vs)
		require.Error(t, err)
	})

	t.Run("AddAndDeleteFiles", func(t *testing.T) {
		vs := &VersionSet{}
		fm := FileMeta{Number: 1, Level: 1}
		edit := VersionEdit{AddFiles: []FileMeta{fm}}
		require.NoError(t, edit.Apply(vs))
		require.Len(t, vs.Levels, 2)
		require.Equal(t, fm, vs.Levels[1][0])

		del := VersionEdit{DeleteFiles: []DeletedFileMeta{{Level: 1, Number: 1}}}
		require.NoError(t, del.Apply(vs))
		require.Empty(t, vs.Levels[1])
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		vs := &VersionSet{}
		edit := VersionEdit{DeleteFiles: []DeletedFileMeta{{Level: 2, Number: 5}}}
		require.NoError(t, edit.Apply(vs))
		require.Len(t, vs.Levels, 0)
	})

	t.Run("Metadata", func(t *testing.T) {
		vs := &VersionSet{}
		edit := VersionEdit{
			ComparatorName: "bytes",
			LastSequence:   1,
			NextFileNumber: 2,
			LogNumber:      3,
			PrevLogNumber:  4,
		}
		require.NoError(t, edit.Apply(vs))
		require.Equal(t, "bytes", vs.Comparator)
		require.Equal(t, uint64(1), vs.LastSequence)
		require.Equal(t, uint64(2), vs.NextFileNumber)
		require.Equal(t, uint64(3), vs.LogNumber)
		require.Equal(t, uint64(4), vs.PrevLogNumber)
	})
}

func TestCoalesceNonZero(t *testing.T) {
	type args[T comparable] struct {
		existing T
		new      T
	}
	type testCase[T comparable] struct {
		name string
		args args[T]
		want T
	}

	// --- int tests ---
	intTests := []testCase[int]{
		{
			name: "int: existing is zero, new is non-zero",
			args: args[int]{existing: 0, new: 5},
			want: 5,
		},
		{
			name: "int: existing is non-zero, new is zero",
			args: args[int]{existing: 10, new: 0},
			want: 10,
		},
		{
			name: "int: both are non-zero",
			args: args[int]{existing: 10, new: 5},
			want: 5,
		},
		{
			name: "int: both are zero",
			args: args[int]{existing: 0, new: 0},
			want: 0,
		},
	}
	for _, tt := range intTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coalesceNonZero(tt.args.existing, tt.args.new); got != tt.want {
				t.Errorf("CoalesceNonZero() = %v, want %v", got, tt.want)
			}
		})
	}

	// --- string tests ---
	stringTests := []testCase[string]{
		{
			name: "string: existing is empty, new is non-empty",
			args: args[string]{existing: "", new: "hello"},
			want: "hello",
		},
		{
			name: "string: existing is non-empty, new is empty",
			args: args[string]{existing: "world", new: ""},
			want: "world",
		},
		{
			name: "string: both are non-empty",
			args: args[string]{existing: "world", new: "hello"},
			want: "hello",
		},
		{
			name: "string: both are empty",
			args: args[string]{existing: "", new: ""},
			want: "",
		},
	}
	for _, tt := range stringTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coalesceNonZero(tt.args.existing, tt.args.new); got != tt.want {
				t.Errorf("CoalesceNonZero() = %v, want %v", got, tt.want)
			}
		})
	}

	// --- float64 tests ---
	float64Tests := []testCase[float64]{
		{
			name: "float64: existing is zero, new is non-zero",
			args: args[float64]{existing: 0.0, new: 5.5},
			want: 5.5,
		},
		{
			name: "float64: existing is non-zero, new is zero",
			args: args[float64]{existing: 10.1, new: 0.0},
			want: 10.1,
		},
		{
			name: "float64: both are non-zero",
			args: args[float64]{existing: 10.1, new: 5.5},
			want: 5.5,
		},
		{
			name: "float64: both are zero",
			args: args[float64]{existing: 0.0, new: 0.0},
			want: 0.0,
		},
	}
	for _, tt := range float64Tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coalesceNonZero(tt.args.existing, tt.args.new); got != tt.want {
				t.Errorf("CoalesceNonZero() = %v, want %v", got, tt.want)
			}
		})
	}
}
