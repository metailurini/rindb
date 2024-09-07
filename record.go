package rindb

type Record interface {
	GetKey() Bytes
	GetValue() Bytes
	GetSize() int
}

func CalOnDiskSize(r Record) int {
	return (mdByteSize /* key len size */ +
		mdByteSize /* value len size */ +
		r.GetSize() /* all key&value size */)
}

var _ Record = RecordImpl{}

type RecordImpl struct {
	Key, Value Bytes
}

// GetKey implements Record.
func (r RecordImpl) GetKey() Bytes {
	return r.Key
}

// GetValue implements Record.
func (r RecordImpl) GetValue() Bytes {
	return r.Value
}

// GetSize implements Record.
func (r RecordImpl) GetSize() int {
	return len(r.GetKey()) + len(r.GetValue())
}
