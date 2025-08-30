package rindb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSSTableBuilder_BuildWithoutAdd(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	builder, err := NewSSTableBuilder(ctx, cfg, fs)
	assert.NoError(t, err)

	_, _, err = builder.Build(ctx)
	assert.EqualError(t, err, "no records to build")
}
