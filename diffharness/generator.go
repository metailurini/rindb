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

const maxKnownKeys = 100

type KeyTracker struct {
	keys   []string
	keySet map[string]struct{}
}

func (kt *KeyTracker) Add(k []byte) {
	if kt.keySet == nil {
		kt.keySet = make(map[string]struct{})
	}
	s := string(k)
	if _, ok := kt.keySet[s]; ok {
		return
	}
	if len(kt.keys) >= maxKnownKeys {
		oldest := kt.keys[0]
		copy(kt.keys, kt.keys[1:])
		kt.keys[len(kt.keys)-1] = ""
		kt.keys = kt.keys[:len(kt.keys)-1]
		delete(kt.keySet, oldest)
	}
	kt.keys = append(kt.keys, s)
	kt.keySet[s] = struct{}{}
}

func (kt *KeyTracker) Del(k []byte) {
	if kt.keySet == nil {
		return
	}
	s := string(k)
	if _, ok := kt.keySet[s]; !ok {
		return
	}
	delete(kt.keySet, s)
	for i, v := range kt.keys {
		if v == s {
			copy(kt.keys[i:], kt.keys[i+1:])
			kt.keys[len(kt.keys)-1] = ""
			kt.keys = kt.keys[:len(kt.keys)-1]
			break
		}
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

func pickSnapshot(r *rand.Rand, seq uint64, snaps []uint64) uint64 {
	if len(snaps) == 0 || r.Intn(10) == 0 {
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
		s := pickSnapshot(r, seq, snaps)
		return GetOp{K: key, SnapSeq: s}
	case OpRange:
		lo := randKey(r, ro.cfg.KeyLen)
		hi := randKey(r, ro.cfg.KeyLen)
		for bytes.Compare(hi, lo) <= 0 {
			hi = randKey(r, ro.cfg.KeyLen)
		}
		s := pickSnapshot(r, seq, snaps)
		return RangeOp{Lo: lo, Hi: hi, SnapSeq: s, Limit: ro.cfg.RangeMax}
	case OpSnap:
		return SnapOp{}
	default:
		panic("unknown op kind")
	}
}
