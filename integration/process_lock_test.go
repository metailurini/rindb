//go:build integration

package rindb_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestProcessLock_HelperProcess(t *testing.T) {
	if os.Getenv("RINDB_HELPER_PROCESS") != "1" {
		return
	}

	dir := os.Getenv("RINDB_HELPER_DIR")
	if dir == "" {
		fmt.Fprintln(os.Stderr, "helper: missing RINDB_HELPER_DIR")
		os.Exit(2)
	}

	disable := os.Getenv("RINDB_HELPER_DISABLE") == "1"
	hold := os.Getenv("RINDB_HELPER_HOLD") == "1"

	opts := []rindb.Option{rindb.WithDatabaseDir(dir)}
	if disable {
		opts = append(opts, rindb.WithDisableProcessLock())
	}

	db, err := rindb.InitRinDB(context.Background(), opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init: %v\n", err)
		os.Exit(1)
	}

	if hold {
		if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
			fmt.Fprintf(os.Stderr, "ready: %v\n", err)
			os.Exit(1)
		}
		if _, err := io.ReadAll(os.Stdin); err != nil {
			fmt.Fprintf(os.Stderr, "wait: %v\n", err)
			os.Exit(1)
		}
	}

	if err := db.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func TestInitRinDB_ProcessLockExclusive(t *testing.T) {
	dir := t.TempDir()

	holder := helperCommand(t, dir, true, false)
	stdout, err := holder.StdoutPipe()
	require.NoError(t, err)
	stdin, err := holder.StdinPipe()
	require.NoError(t, err)

	holderWaited := false
	require.NoError(t, holder.Start())
	t.Cleanup(func() {
		if holder.Process != nil && !holderWaited {
			_ = holder.Process.Kill()
			_ = holder.Wait()
		}
	})
	waitForHelperReady(t, stdout)

	contender := helperCommand(t, dir, false, false)
	output, err := contender.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "database is already open")

	require.NoError(t, stdin.Close())
	require.NoError(t, holder.Wait())
	holderWaited = true
}

func TestInitRinDB_DisableProcessLockAllowsParallel(t *testing.T) {
	dir := t.TempDir()

	holder := helperCommand(t, dir, true, true)
	stdout, err := holder.StdoutPipe()
	require.NoError(t, err)
	stdin, err := holder.StdinPipe()
	require.NoError(t, err)

	holderWaited := false
	require.NoError(t, holder.Start())
	t.Cleanup(func() {
		if holder.Process != nil && !holderWaited {
			_ = holder.Process.Kill()
			_ = holder.Wait()
		}
	})
	waitForHelperReady(t, stdout)

	contender := helperCommand(t, dir, false, true)
	output, err := contender.CombinedOutput()
	require.NoErrorf(t, err, "helper output: %s", string(output))

	require.NoError(t, stdin.Close())
	require.NoError(t, holder.Wait())
	holderWaited = true
}

func helperCommand(t *testing.T, dir string, hold bool, disable bool) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=TestProcessLock_HelperProcess", "--")
	env := append(os.Environ(),
		"GO_WANT_HELPER_PROCESS=1",
		"RINDB_HELPER_PROCESS=1",
		"RINDB_HELPER_DIR="+dir,
	)
	if hold {
		env = append(env, "RINDB_HELPER_HOLD=1")
	}
	if disable {
		env = append(env, "RINDB_HELPER_DISABLE=1")
	}
	cmd.Env = env
	return cmd
}

func waitForHelperReady(t *testing.T, r io.Reader) {
	t.Helper()

	reader := bufio.NewReader(r)
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)
}
