# Descending range iteration – suspicious code paths

The following code snippets look fragile for `RangeDesc` scans and merit
closer inspection:

1. **`RangeIterator.primePrevWithOrder` reuses `MergingIterator.Next` when
   scanning backwards in descending mode.** When `order == RangeDesc` the
   helper fetches candidates through `r.mi.Next()` instead of using the
   reverse heap, so it bypasses the duplicate/tombstone filtering applied in
   `collapseDescendingRun`. The staging code then relies solely on
   `stagePrevCandidate`, which was tuned for ascending pivots, to drop stale
   versions. Any mismatch between the two code paths could leak deleted
   values or re‑surface older sequence numbers when oscillating between
   `Next` and `Prev`. 【F:range.go†L232-L268】

2. **`RangeIterator.Last` forces `searchOrder := RangeAsc` for every
   iterator.** Even when a client explicitly requests `RangeDesc`, the tail
   walk uses the ascending `Prev` plumbing, including `r.primePrevWithOrder`
   with `order == RangeAsc`. That means descending snapshots depend on the
   ascending anchor rules when priming their last element. If the reverse
   caches or crossing‑anchor state differ between orientations, `Last`
   could miss the true tail record or double‑emit the boundary key after
   oscillations. 【F:range.go†L330-L395】

3. **`MergingIterator.prepareNext` skips advancing child iterators while
   resuming a descending walk.** The `skipAdvance := m.order == RangeDesc &&
   !m.forward` guard prevents fetching a successor from `item.iter.Next()`
   when the merged iterator most recently moved backward. In a descending
   scan that performs `Prev` then `Next`, this leaves the child iterator
   parked on the already returned key; the forward heap is re‑queued with
   the same `pqItem`, so the next `Next` call may replay the old record
   instead of stepping to the next lower key. 【F:merging_iterator.go†L398-L415】

4. **`MergingIterator.commitPeekedReverse` drains every predecessor with the
   same key before breaking.** The inner loop keeps calling `Prev()` while
   `item.iter.HasPrev()` and pushes each result onto the reverse heap, but it
   stops only once the key changes. When multiple iterators share the same
   user key, this eager drain could interleave copies of the same record on
   the reverse heap without deduplicating by sequence number, especially if
   `Prev()` rewinds past versions that the descending collapse logic expects
   to filter lazily. 【F:merging_iterator.go†L212-L235】

Each of these paths interacts directly with the asc/desc direction flags;
unit coverage that exercises complex direction flips across multiple
underlying iterators should highlight whether they are benign or real bugs.
