package rindb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeNumberTest(buf *bytes.Buffer, n uint64) {
	var b [mdByteSize]byte
	byteOrder.PutUint64(b[:], n)
	buf.Write(b[:])
}

func buildRecordBytes(key Bytes, seq uint64, value Bytes) []byte {
	ikey := EncodeInternalKey(key, seq, TypeValue)
	var buf bytes.Buffer
	writeNumberTest(&buf, uint64(len(ikey)))
	writeNumberTest(&buf, uint64(len(value)))
	buf.Write(ikey)
	buf.Write(value)
	var c [checksumSize]byte
	byteOrder.PutUint32(c[:], checksum(ikey, value))
	buf.Write(c[:])
	return buf.Bytes()
}

func TestScanBlockForPrevEOF(t *testing.T) {
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	rec := buildRecordBytes(Bytes("a"), 1, Bytes("v"))
	_, err := fs.WriteAt(rec, 0)
	require.NoError(t, err)

	s := &SStable{FileSystem: fs}
	srr := sstableIRangeRev{s: s, startKey: Bytes("a"), endKey: Bytes("z"), seq: ^uint64(0)}

	meta, off, ok, err := srr.scanBlockForPrev(0, int64(len(rec)+8))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, Bytes("a"), meta.key)
	assert.Equal(t, int64(0), off)
}

func TestScanBlockForPrevEndKeyBreak(t *testing.T) {
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	rec1 := buildRecordBytes(Bytes("a"), 1, Bytes("v"))
	rec2 := buildRecordBytes(Bytes("d"), 1, Bytes("v"))
	data := append(rec1, rec2...)
	_, err := fs.WriteAt(data, 0)
	require.NoError(t, err)

	s := &SStable{FileSystem: fs}
	srr := sstableIRangeRev{s: s, startKey: Bytes("a"), endKey: Bytes("c"), seq: ^uint64(0)}

	meta, off, ok, err := srr.scanBlockForPrev(0, int64(len(data)))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, Bytes("a"), meta.key)
	assert.Equal(t, int64(0), off)
}
