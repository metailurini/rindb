package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChecksumCRC32C(t *testing.T) {
	data := []byte("hello world")
	expected := uint32(0xc99465aa)
	actual := checksum(data)
	assert.Equal(t, expected, actual)
}
