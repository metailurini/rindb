package rindb

type prefetchState struct {
	records [2]Record
	ready   [2]bool
}

func (p *prefetchState) Has(dir Direction) bool {
	return p.ready[dirIndex(dir)]
}

func (p *prefetchState) Peek(dir Direction) (Record, bool) {
	idx := dirIndex(dir)
	if !p.ready[idx] {
		return nil, false
	}
	return p.records[idx], true
}

func (p *prefetchState) Stage(dir Direction, rec Record) {
	idx := dirIndex(dir)
	if rec == nil {
		p.records[idx] = nil
		p.ready[idx] = false
		return
	}
	p.records[idx] = rec
	p.ready[idx] = true
}

func (p *prefetchState) Pop(dir Direction) (Record, bool) {
	idx := dirIndex(dir)
	if !p.ready[idx] {
		return nil, false
	}
	rec := p.records[idx]
	p.records[idx] = nil
	p.ready[idx] = false
	return rec, true
}

func (p *prefetchState) Clear(dir Direction) {
	idx := dirIndex(dir)
	p.records[idx] = nil
	p.ready[idx] = false
}

func (p *prefetchState) ClearAll() {
	p.Clear(DirForward)
	p.Clear(DirReverse)
}
