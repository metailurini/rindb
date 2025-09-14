package diffharness

import (
	"encoding/json"
	"os"
)

// OpKind enumerates supported operation types.
type OpKind uint8

const (
	OpPut OpKind = iota
	OpDel
	OpGet
	OpRange
	OpSnap
)

const OpInvariantCheck OpKind = 255

// Op models a single diffharness operation.
type Op struct {
	Kind    OpKind
	K       []byte
	V       []byte
	Lo      []byte
	Hi      []byte
	SnapSeq uint64
	Limit   int
}

type Phase string

const (
	PhasePrepared  Phase = "prepared"
	PhaseMyDone    Phase = "my_done"
	PhaseRefDone   Phase = "ref_done"
	PhaseCommitted Phase = "committed"
)

// Harness coordinates both engines and records every executed operation.
type Harness struct {
	Seed int64

	My  Engine
	Ref *SQLiteOracle

	Seq       uint64
	Snapshots []uint64

	keys   []string
	keySet map[string]struct{}

	log *os.File
	enc *json.Encoder
	ops int

	crash     func() error
	telemetry func(seq uint64, ops int)
}

// Cfg controls random operation generation.
type Cfg struct {
	KeyLen    int
	ValLenMin int
	ValLenMax int
	RangeMax  int
	Weights   map[OpKind]int

	CrashEvery     int
	TelemetryEvery int
}
