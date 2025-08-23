//go:build integration

package rindb_test

import (
	"context"
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

	return &db, cleanup
}
