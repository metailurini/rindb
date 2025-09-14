package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckDir_FindsMisnamed(t *testing.T) {
	dir := t.TempDir()
	src := `package a
import "testing"
func TestGood_Behavior(t *testing.T) {}
func Testbad(t *testing.T) {}
`
	file := filepath.Join(dir, "a_test.go")
	require.NoError(t, os.WriteFile(file, []byte(src), 0o644))
	names, err := checkDir(dir)
	require.NoError(t, err)
	require.Equal(t, []string{file + ":4: Testbad"}, names)
}

func TestCheckDir_AllGood(t *testing.T) {
	dir := t.TempDir()
	src := `package a
import "testing"
func TestAlpha_Beta(t *testing.T) {}
`
	file := filepath.Join(dir, "a_test.go")
	require.NoError(t, os.WriteFile(file, []byte(src), 0o644))
	names, err := checkDir(dir)
	require.NoError(t, err)
	require.Len(t, names, 0)
}

func TestCheckDir_IgnoresNonTestFiles(t *testing.T) {
	dir := t.TempDir()
	src := `package a
import "testing"
func Testbad(t *testing.T) {}
`
	file := filepath.Join(dir, "a.go")
	require.NoError(t, os.WriteFile(file, []byte(src), 0o644))
	names, err := checkDir(dir)
	require.NoError(t, err)
	require.Len(t, names, 0)
}
