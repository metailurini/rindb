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
	GetSequenceNumber() uint64
	GetType() RecordType
}

func CalOnDiskSize(r Record) int {
	return (mdByteSize /* internal key len size */ +
		mdByteSize /* value len size */ +
		len(r.GetKey()) /* user key */ +
		internalKeySuffixLen /* seq+type */ +
		len(r.GetValue()) /* value */ +
		checksumSize /* checksum */ +
		mdByteSize /* record size trailer */)
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

// GetSequenceNumber implements Record.
func (r RecordImpl) GetSequenceNumber() uint64 {
	return r.SequenceNumber
}

// GetType implements Record.
func (r RecordImpl) GetType() RecordType {
	return r.Type
}

// cloneRecord produces a deep copy of the provided record's key and value.
func cloneRecord(rec Record) Record {
	if rec == nil {
		return nil
	}
	return RecordImpl{
		Key:            rec.GetKey().Clone(),
		Value:          rec.GetValue().Clone(),
		SequenceNumber: rec.GetSequenceNumber(),
		Type:           rec.GetType(),
	}
}
