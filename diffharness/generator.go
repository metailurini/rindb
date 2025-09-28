package diffharness

import (
	"bytes"
	"math/rand"
	"sort"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randPrefixedBytes(r *rand.Rand, n int, prefix, suffix string) []byte {
	minLen := len(prefix) + len(suffix)
	if n < minLen {
		n = minLen
	}
	b := make([]byte, n)
	copy(b, prefix)
	for i := len(prefix); i < n-len(suffix); i++ {
		b[i] = letters[r.Intn(len(letters))]
	}
	copy(b[n-len(suffix):], suffix)
	return b
}

func randKey(r *rand.Rand, n int) []byte { return randPrefixedBytes(r, n, "sk", "ek") }

func randValue(r *rand.Rand, n int) []byte { return randPrefixedBytes(r, n, "sv", "ev") }

// KeyTracker tracks up to max recently seen keys.
type KeyTracker struct {
	keys  []string
	index map[string]int
	max   int
	next  int
}

// NewKeyTracker initializes a KeyTracker with a maximum size.
func NewKeyTracker(max int) KeyTracker {
	if max <= 0 {
		max = 100
	}
	return KeyTracker{max: max, index: make(map[string]int, max)}
}

func (kt *KeyTracker) Add(k []byte) {
	if kt.index == nil {
		kt.index = make(map[string]int, kt.max)
	}
	s := string(k)
	if _, ok := kt.index[s]; ok {
		return
	}
	if len(kt.keys) < kt.max {
		kt.keys = append(kt.keys, s)
		kt.index[s] = len(kt.keys) - 1
		return
	}
	victim := kt.keys[kt.next]
	delete(kt.index, victim)
	kt.keys[kt.next] = s
	kt.index[s] = kt.next
	kt.next++
	if kt.next >= kt.max {
		kt.next = 0
	}
}

func (kt *KeyTracker) Del(k []byte) {
	if kt.index == nil {
		return
	}
	s := string(k)
	idx, ok := kt.index[s]
	if !ok {
		return
	}
	last := len(kt.keys) - 1
	if idx != last {
		kt.keys[idx] = kt.keys[last]
		kt.index[kt.keys[idx]] = idx
	}
	kt.keys[last] = ""
	kt.keys = kt.keys[:last]
	delete(kt.index, s)
	if kt.next > idx {
		kt.next--
	}
	if len(kt.keys) > 0 {
		kt.next %= len(kt.keys)
	} else {
		kt.next = 0
	}
}

func (kt *KeyTracker) Random(r *rand.Rand) []byte {
	if len(kt.keys) == 0 {
		return nil
	}
	return []byte(kt.keys[r.Intn(len(kt.keys))])
}

type RandOps struct{ cfg Cfg }

func (ro RandOps) genKey(r *rand.Rand, kt *KeyTracker) []byte {
	k := randKey(r, ro.cfg.KeyLen)
	if kt != nil {
		if ex := kt.Random(r); ex != nil && r.Intn(2) == 0 {
			k = ex
		}
	}
	return k
}

func pickSnapshot(r *rand.Rand, seq uint64, snaps []uint64, reuseEvery int) uint64 {
	if reuseEvery <= 0 {
		reuseEvery = 10
	}
	if len(snaps) == 0 || r.Intn(reuseEvery) == 0 {
		return seq
	}
	return snaps[r.Intn(len(snaps))]
}

func (ro RandOps) Next(r *rand.Rand, kt *KeyTracker, seq uint64, snaps []uint64) Operation {
	sum := 0
	var kinds []OpKind
	for op, w := range ro.cfg.Weights {
		sum += w
		kinds = append(kinds, op)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	x := r.Intn(sum)
	var kind OpKind
	for _, op := range kinds {
		w := ro.cfg.Weights[op]
		if x < w {
			kind = op
			break
		}
		x -= w
	}
	switch kind {
	case OpPut:
		vlen := ro.cfg.ValLenMin + r.Intn(ro.cfg.ValLenMax-ro.cfg.ValLenMin+1)
		key := ro.genKey(r, kt)
		return PutOp{K: key, V: randValue(r, vlen)}
	case OpDel:
		key := ro.genKey(r, kt)
		return DelOp{K: key}
	case OpGet:
		key := ro.genKey(r, kt)
		s := pickSnapshot(r, seq, snaps, ro.cfg.SnapshotReuseEvery)
		return GetOp{K: key, SnapSeq: s}
	case OpRange:
		lo := randKey(r, ro.cfg.KeyLen)
		hi := randKey(r, ro.cfg.KeyLen)
		for bytes.Compare(hi, lo) <= 0 {
			hi = randKey(r, ro.cfg.KeyLen)
		}
		s := pickSnapshot(r, seq, snaps, ro.cfg.SnapshotReuseEvery)
		orders := ro.cfg.rangeOrders()
		order := orders[r.Intn(len(orders))]
		return RangeOp{Lo: lo, Hi: hi, Order: order, SnapSeq: s, Limit: ro.cfg.RangeMax}
	case OpSnap:
		return SnapOp{}
	default:
		panic("unknown op kind")
	}
}
