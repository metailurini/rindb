package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChecksumCRC32C(t *testing.T) {
	cases := []struct {
		name  string
		parts [][]byte
	}{
		{
			name:  "single slice",
			parts: [][]byte{[]byte("hello world")},
		},
		{
			name:  "multiple slices",
			parts: [][]byte{[]byte("hello"), []byte(" world")},
		},
	}
	expected := uint32(0xc99465aa)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := checksum(tc.parts...)
			assert.Equal(t, expected, actual)
		})
	}
}
