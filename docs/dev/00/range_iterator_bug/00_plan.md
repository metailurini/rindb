# RangeIterator Prev-direction Bug Fix Plan

## Review Highlights (Context)
- The deduplication state in [`range.go`](../../../../range.go) tracks the last surfaced key via `lastKey`/`lastKeySet` but both `HasPrev` and `Prev` clear `lastKeySet` whenever `r.forward` is true.
- Because `Prev()` resets the flag and immediately returns, the subsequent `preparePrev()` call cannot see the just-returned key and exposes stale record versions.

```go
func (r *RangeIterator) Prev() (Record, error) {
    if r.forward {
        r.lastKeySet = false // resets the guard before preparePrev can run again
    }
    ...
}
```

## Step-by-step Plan
1. **Reproduce the regression with a focused test** *(Complexity: 4/10 — Dedicated plan file: not required)*
   - Extend `range_test.go` with a scenario that performs `Next()` followed by `Prev()` across versions of the same user key and asserts only the latest visible value is returned.
   - Use descriptive helper names (e.g., `mustNextValue`, `mustPrevValue`) to spotlight the iteration order and ensure the failing behavior is deterministic.

   ```go
   func TestRangeIteratorPrev_SuppressesOlderVersion(t *testing.T) {
       iter := buildRangeIter(t, []kv{{"k", "v3", seq(3)}}, []kv{{"k", "v2", seq(2)}})
       mustNextValue(t, iter, "k", "v3")
       mustPrevEOI(t, iter) // current bug surfaces v2 instead of EOI
   }
   ```

2. **Adjust RangeIterator state transitions for bidirectional stability** *(Complexity: 6/10 — Dedicated plan file: not required)*
   - Refactor `preparePrev`, `Prev`, and the forward/backward toggles so that `lastKey` persists until the opposite direction yields a different user key, guarding against duplicate exposure.
   - Track the record just surfaced by `Next()` in a `crossingAnchor` slot so the backward path can consume it without re-exposing older versions.
   - Introduce a helper (sketched below as `preparePrevCrossing`) that preloads the `prev` slot when crossing from forward iteration, effectively mirroring `prepareNext`'s direction-change handling.
   - Mirror the `prepareNext` logic: when switching direction, seed `lastKey` from the record being returned instead of clearing the flag prematurely.

   ```go
   func (r *RangeIterator) Prev() (Record, error) {
       if r.forward {
           if !r.prevPrepared && !r.preparePrevCrossing() {
               return empty, EOI
           }
           r.forward = false
       }
       rec := r.prev
       r.lastKey = rec.GetKey().Clone()
       r.lastKeySet = true
       r.prevPrepared = false
       return rec, nil
   }
   ```

   ```go
   func (r *RangeIterator) preparePrevCrossing() bool {
       if !r.crossingAnchorSet {
           r.preparePrev()
           return r.prevPrepared
       }

       // The anchor holds the record most recently emitted by Next(); place it into the prev slot so the
       // following call to preparePrev can skip over it while keeping lastKey intact.
       r.prev = r.crossingAnchor
       r.prevPrepared = true
       r.crossingAnchorSet = false

       r.preparePrev()
       return r.prevPrepared
   }
   ```

3. **Broaden coverage and validate iterator invariants** *(Complexity: 3/10 — Dedicated plan file: not required)*
   - Add alternating-direction test cases (including multi-key spans) to `integration/range_iterator_next_prev_test.go` to confirm deduplication consistency through complex sequences.
   - Run `make test` and `make test-integration-smoke` to ensure no regressions; capture results in the eventual PR.

   ```go
   func TestRangeIterator_AlternatingSequences(t *testing.T) {
       seq := []op{{next: "a:2"}, {prev: "a:2"}, {prevEOI: true}, {next: "a:2"}}
       assertIteration(t, iter, seq)
   }
   ```

## Dedicated Plan Files
- None of the steps currently require a standalone plan document; update this section if future scope increases.
