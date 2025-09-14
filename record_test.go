package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecord_OnDiskSize(t *testing.T) {
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
			args: args{newRecord(Bytes("key"), Bytes("value"), 1)},
			want: 37,
		},
		{
			name: "Empty key and value",
			args: args{newRecord(Bytes(nil), Bytes(nil), 18446744073709551615)},
			want: 29,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalOnDiskSize(tt.args.r)
			assert.Equal(t, tt.want, got)
		})
	}
}
