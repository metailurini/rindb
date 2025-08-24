//go:build integration

package rindb_test

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func initTestDB(t *testing.T, opts ...rindb.Option) (*rindb.Rindb, func()) {
	t.Helper()

	dir := t.TempDir()
	opts = append([]rindb.Option{rindb.WithDatabaseDir(dir)}, opts...)

	db, err := rindb.InitRinDB(context.Background(), opts...)
	require.NoError(t, err)

	cleanup := func() { require.NoError(t, db.Close()) }

	return db, cleanup
}

// countNumericEntriesInFDDirectory counts the number of numeric entries in the file descriptor directory for the current process.
// This function is specifically designed to avoid `lstat` calls on macOS,
// which can sometimes result in "bad file descriptor" errors when iterating /dev/fd.
//
// On Linux, it reads from /proc/self/fd.
// On macOS, it reads from /dev/fd.
//
// Note: The os.Open(dir) operation itself consumes a file descriptor that is included in the returned count.
// This function subtracts 1 from the count to exclude its own file descriptor, providing a more accurate measure of external FDs.
func countNumericEntriesInFDDirectory(t *testing.T) int {
	t.Helper()

	// Determine the appropriate directory for file descriptors based on the OS.
	dir := "/proc/self/fd"
	if runtime.GOOS == "darwin" {
		dir = "/dev/fd"
	}

	// Open the directory containing file descriptors.
	f, err := os.Open(dir)
	require.NoError(t, err)
	defer f.Close() // Ensure the directory handle is closed.

	// Read the names of the entries in the directory.
	// Using Readdirnames(-1) reads all entries and avoids lstat calls,
	// which is crucial for robustness on macOS.
	names, err := f.Readdirnames(-1)
	require.NoError(t, err)

	count := 0
	// Iterate through the entry names and count only those that are numeric.
	// This counts entries whose names are numeric strings, which are typically file descriptors.
	// It does not verify the existence, validity, or actual open status of a file descriptor.
	// This approach is used to avoid `lstat` calls on macOS, which can cause "bad file descriptor" errors.
	for _, n := range names {
		if _, err := strconv.Atoi(n); err == nil {
			count++ // Only count numeric entries (actual FDs).
		}
	}
	// Subtract 1 to exclude the file descriptor opened by os.Open(dir) itself.
	return count - 1
}
