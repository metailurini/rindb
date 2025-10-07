package rindb

// prefetchState tracks speculative records staged for forward and reverse
// traversal. It allows the iterator to peek ahead without advancing the
// underlying merging iterator so direction switches can reuse buffered items.
type prefetchState struct {
	records [2]Record
	ready   [2]bool
}

// has reports whether a staged record exists for the supplied direction.
func (p *prefetchState) has(dir Direction) bool {
	return p.ready[dirIndex(dir)]
}

// peek returns the staged record for the provided direction without consuming
// it. The boolean result reports whether a record was available.
func (p *prefetchState) peek(dir Direction) (Record, bool) {
	idx := dirIndex(dir)
	if !p.ready[idx] {
		return nil, false
	}
	return p.records[idx], true
}

// stage updates the staged record for the given direction. Passing a nil record
// clears the staged entry.
func (p *prefetchState) stage(dir Direction, rec Record) {
	idx := dirIndex(dir)
	if rec == nil {
		p.records[idx] = nil
		p.ready[idx] = false
		return
	}
	p.records[idx] = rec
	p.ready[idx] = true
}

// pop retrieves and clears the staged record for the requested direction.
func (p *prefetchState) pop(dir Direction) (Record, bool) {
	idx := dirIndex(dir)
	if !p.ready[idx] {
		return nil, false
	}
	rec := p.records[idx]
	p.records[idx] = nil
	p.ready[idx] = false
	return rec, true
}

// clear removes any staged record for the supplied direction.
func (p *prefetchState) clear(dir Direction) {
	idx := dirIndex(dir)
	p.records[idx] = nil
	p.ready[idx] = false
}

// clearAll clears both forward and reverse staged records.
func (p *prefetchState) clearAll() {
	p.clear(DirForward)
	p.clear(DirReverse)
}
