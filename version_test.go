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
