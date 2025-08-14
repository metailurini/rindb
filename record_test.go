package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalOnDiskSize(t *testing.T) {
	type args struct {
		r Record
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Key and value",
			args: args{RecordImpl{Bytes("key"), Bytes("value"), 1}},
			want: 32,
		},
		{
			name: "Empty key and value",
			args: args{RecordImpl{Bytes(nil), Bytes(nil), 18446744073709551615}},
			want: 24,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalOnDiskSize(tt.args.r)
			assert.Equal(t, tt.want, got)
		})
	}
}
