package rindb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type coalesceArgs[T comparable] struct {
	existing T
	new      T
}

type coalesceTestCase[T comparable] struct {
	name string
	args coalesceArgs[T]
	want T
}

func TestVersion_Edit(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "ComparatorMismatch",
			run: func(t *testing.T) {
				vs := &versionSet{Comparator: "bytes"}
				edit := versionEdit{ComparatorName: "rev"}
				err := edit.apply(vs)
				require.Error(t, err)
			},
		},
		{
			name: "AddAndDeleteFiles",
			run: func(t *testing.T) {
				vs := &versionSet{}
				fm := fileMeta{Number: 1, Level: 1}
				edit := versionEdit{AddFiles: []fileMeta{fm}}
				require.NoError(t, edit.apply(vs))
				require.Len(t, vs.Levels, 2)
				require.Equal(t, fm, vs.Levels[1][0])

				del := versionEdit{DeleteFiles: []deletedFileMeta{{Level: 1, Number: 1}}}
				require.NoError(t, del.apply(vs))
				require.Empty(t, vs.Levels[1])
			},
		},
		{
			name: "AddFilesSorted",
			run: func(t *testing.T) {
				vs := &versionSet{}
				fm1 := fileMeta{Number: 1, Level: 0, Smallest: InternalKey{UserKey: Bytes("b"), Seq: 1, Type: TypeValue}}
				fm2 := fileMeta{Number: 2, Level: 0, Smallest: InternalKey{UserKey: Bytes("a"), Seq: 1, Type: TypeValue}}
				edit := versionEdit{AddFiles: []fileMeta{fm1, fm2}}
				require.NoError(t, edit.apply(vs))
				require.Len(t, vs.Levels[0], 2)
				require.Equal(t, uint64(2), vs.Levels[0][0].Number)
				require.Equal(t, uint64(1), vs.Levels[0][1].Number)
			},
		},
		{
			name: "DeleteNonExistent",
			run: func(t *testing.T) {
				vs := &versionSet{}
				edit := versionEdit{DeleteFiles: []deletedFileMeta{{Level: 2, Number: 5}}}
				require.NoError(t, edit.apply(vs))
				require.Len(t, vs.Levels, 0)
			},
		},
		{
			name: "Metadata",
			run: func(t *testing.T) {
				vs := &versionSet{}
				edit := versionEdit{
					ComparatorName: "bytes",
					LastSequence:   1,
					NextFileNumber: 2,
					LogNumber:      3,
					PrevLogNumber:  4,
				}
				require.NoError(t, edit.apply(vs))
				require.Equal(t, "bytes", vs.Comparator)
				require.Equal(t, uint64(1), vs.LastSequence)
				require.Equal(t, uint64(2), vs.NextFileNumber)
				require.Equal(t, uint64(3), vs.LogNumber)
				require.Equal(t, uint64(4), vs.PrevLogNumber)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestCoalesce_NonZero(t *testing.T) {
	intCases := []coalesceTestCase[int]{
		{
			name: "int: existing is zero, new is non-zero",
			args: coalesceArgs[int]{existing: 0, new: 5},
			want: 5,
		},
		{
			name: "int: existing is non-zero, new is zero",
			args: coalesceArgs[int]{existing: 10, new: 0},
			want: 10,
		},
		{
			name: "int: both are non-zero",
			args: coalesceArgs[int]{existing: 10, new: 5},
			want: 5,
		},
		{
			name: "int: both are zero",
			args: coalesceArgs[int]{existing: 0, new: 0},
			want: 0,
		},
	}
	runCoalesceTests(t, intCases)

	stringCases := []coalesceTestCase[string]{
		{
			name: "string: existing is empty, new is non-empty",
			args: coalesceArgs[string]{existing: "", new: "hello"},
			want: "hello",
		},
		{
			name: "string: existing is non-empty, new is empty",
			args: coalesceArgs[string]{existing: "world", new: ""},
			want: "world",
		},
		{
			name: "string: both are non-empty",
			args: coalesceArgs[string]{existing: "world", new: "hello"},
			want: "hello",
		},
		{
			name: "string: both are empty",
			args: coalesceArgs[string]{existing: "", new: ""},
			want: "",
		},
	}
	runCoalesceTests(t, stringCases)

	float64Cases := []coalesceTestCase[float64]{
		{
			name: "float64: existing is zero, new is non-zero",
			args: coalesceArgs[float64]{existing: 0.0, new: 5.5},
			want: 5.5,
		},
		{
			name: "float64: existing is non-zero, new is zero",
			args: coalesceArgs[float64]{existing: 10.1, new: 0.0},
			want: 10.1,
		},
		{
			name: "float64: both are non-zero",
			args: coalesceArgs[float64]{existing: 10.1, new: 5.5},
			want: 5.5,
		},
		{
			name: "float64: both are zero",
			args: coalesceArgs[float64]{existing: 0.0, new: 0.0},
			want: 0.0,
		},
	}
	runCoalesceTests(t, float64Cases)
}

func runCoalesceTests[T comparable](t *testing.T, cases []coalesceTestCase[T]) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := coalesceNonZero(tt.args.existing, tt.args.new); got != tt.want {
				t.Errorf("CoalesceNonZero() = %v, want %v", got, tt.want)
			}
		})
	}
}
