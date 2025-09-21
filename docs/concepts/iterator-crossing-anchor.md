# Iterator Crossing Anchors

The storage engine exposes bidirectional range scans through two layers of iterators:

* `MergingIterator` merges multiple sorted sources, exposing every `Record` in key order while keeping the newest sequence number for a key ahead of older versions.
* `RangeIterator` is the user-facing wrapper that hides tombstones and coalesces duplicate keys into a single logical entry.

Both iterators cooperate to guarantee that the boundary element between forward and backward scans appears **exactly once** when callers alternate between `Next` and `Prev`. This behaviour relies on a shared concept called the _crossing anchor_.

## Why an anchor is required

Consider a caller that reads a record with `Next`, immediately rewinds with `Prev`, and then goes forward again:

1. `Next` should return the record and advance.
2. `Prev` should return the same record so the caller can "rewind" one position.
3. The following `Next` should surface the record again before continuing forward.

Without special handling, naïvely merging the underlying streams would either return the boundary record twice in a row or skip it entirely because `Next` and `Prev` draw from separate priority queues. The crossing anchor lets both iterators remember the record that straddled the direction change so they can treat it as a special case exactly once.

## Crossing anchor state in `MergingIterator`

`MergingIterator` owns two priority queues: `fwd` for forward traversal and `rev` for backward traversal. Every time the iterator successfully surfaces an item, it captures it into:

* `crossingAnchor` – a `pqItem` holding the record and the child iterator that produced it.
* `crossingAnchorSet` – a boolean guard so `nil` or zero values are not misinterpreted.
* `forward` – tracks the **most recent** direction of travel (`true` after `Next`, `false` after `Prev`).

Because the anchor stores the originating iterator alongside the key, sequence number, and type, `matchesCrossingAnchor` can later identify the exact record that was last emitted.

### Setting the anchor

`Next` and `Prev` both set the anchor immediately before returning the chosen record to the caller. This happens after the directional bookkeeping (`forward = true/false`) and after any prepared state for the opposite direction is discarded so that stale queue entries cannot be replayed accidentally.

### Using the anchor while preparing results

`prepareNext` and `preparePrev` drive the priority queues until they have a candidate to expose. When a popped item equals the current anchor, the iterator inspects the `forward` flag to decide whether the caller is continuing in the same direction or has just flipped:

* Continuing in the same direction means the boundary record has already been delivered, so the candidate is skipped and the loop continues.
* Detecting a direction flip clears the anchor and lets the current candidate through exactly once. This allows the first `Next` after a `Prev` (and vice versa) to replay the boundary record.

As soon as an item survives the anchor check, it becomes the prepared result and is pushed onto the opposite-direction queue so the iterator can walk back over it later. Any errors encountered while refilling child iterators are recorded but deferred until the next consumer-facing call.

## Coordinating with `RangeIterator`

`RangeIterator` adds higher-level semantics on top of the merged stream. It must deduplicate multiple physical records with the same user key and filter out deletions. These responsibilities would normally prevent the boundary element from replaying after a direction change, because the "duplicate" key would be skipped.

To stay aligned with `MergingIterator`, `RangeIterator` maintains its own `crossingAnchor` containing only the record (there is no nested iterator to track). Its `matchesCrossingAnchor` helper uses key, sequence number, and type comparisons to recognise the boundary record.

During preparation:

* `prepareNext` discards any record whose key matches the previous key unless the iterator just reversed direction and the record matches the crossing anchor. In that special case it clears the anchor so the boundary record can surface once more.
* `preparePrev` performs the symmetric operation while walking backwards.

When `Next` or `Prev` finally return a record, they update `forward`, store the record as the new anchor, and expose it to the caller.

## Putting it together: an example walk

Imagine two SSTables contributing the following records (newest sequence first):

| Iterator | Records (key@seq:type) |
| --- | --- |
| A | `apple@7:val`, `apple@5:val`, `banana@4:del` |
| B | `banana@6:val`, `carrot@3:val` |

1. `Next` returns `apple@7`. The anchor now references this record (`forward = true`).
2. Another `Next` returns `banana@6`. The record is pushed to the reverse queue.
3. The caller invokes `Prev`. `preparePrev` pops `banana@6`, recognises that the last direction was forward, clears the anchor, pushes the record back onto the forward queue, and stages it. `Prev` returns `banana@6` and sets `forward = false`.
4. A second `Prev` moves to `apple@7`. `preparePrev` in `MergingIterator` pops `apple@7`. This doesn't match the anchor (`banana@6`), so it's passed to `RangeIterator`. `RangeIterator` sees the key has changed from `banana` to `apple`, accepts `apple@7`, and returns it. The user observes `apple@7`. Older versions of the key, like `apple@5`, are skipped by `RangeIterator`'s duplicate filtering.
5. Switching back to `Next`, the reverse process occurs. The boundary record is now `apple@7` (the last record returned). The first `Next` after the direction flip replays `apple@7` exactly once before visiting new keys.

Throughout this sequence the user sees each logical record at most once per direction change, with tombstones hidden and duplicates suppressed, while the internal priority queues remain consistent.

## Extending the iterators safely

When modifying either iterator, keep the following guidelines in mind:

* Always set the crossing anchor _after_ determining the direction of travel for the call so the preparation helpers can reason about the last move.
* If new filtering or de-duplication rules are added, ensure they consult `matchesCrossingAnchor` before skipping a record; otherwise oscillating between directions may start dropping results.
* Any additional state derived from surfaced records (such as cached keys) should be reset when clearing the crossing anchor so that both iterators agree on which element marks the direction boundary.

Following these rules preserves the contract that alternating `Next` and `Prev` operations replay only the single boundary record, providing intuitive iteration semantics to higher layers.
