package rindb

type RecordType uint8

const (
	TypeValue RecordType = iota
	TypeDeletion
	TypeMerge
)

type Record interface {
	GetKey() Bytes
	GetValue() Bytes
	GetSize() int
	GetSequenceNumber() uint64
	GetType() RecordType
}

func CalOnDiskSize(r Record) int {
	return (mdByteSize /* internal key len size */ +
		mdByteSize /* value len size */ +
		len(r.GetKey()) /* user key */ +
		internalKeySuffixLen /* seq+type */ +
		len(r.GetValue()))
}

var _ Record = RecordImpl{}

type RecordImpl struct {
	Key, Value     Bytes
	SequenceNumber uint64
	Type           RecordType
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

// GetSequenceNumber implements Record.
func (r RecordImpl) GetSequenceNumber() uint64 {
	return r.SequenceNumber
}

// GetType implements Record.
func (r RecordImpl) GetType() RecordType {
	return r.Type
}
