# Descending range iterator suspicions

## 1. Peeking reverse entries before tombstone filtering leaks older versions

`RangeIterator.primeNext` always calls `consumePeekedReverse` before it checks
for duplicate keys or tombstones when operating in descending mode. Once the
reverse heap entry is committed, the merging iterator immediately pushes the
candidate into the forward heap and rewinds the child iterator. If the record
turns out to be a tombstone, the key still ends up recorded as the latest
`lastKey`, but the older version that was fetched from another iterator is now
eligible for emission on the next loop iteration because the tombstone was never
left on the reverse heap.【F:range.go†L120-L180】【F:merging_iterator.go†L206-L220】

The new regression `TestRangeIterator_DescendingSkipsTombstonedKey` shows the
leak: after visiting keys `d`, `c`, and `b`, a tombstone for `a` should suppress
all older `a` values, yet the iterator still returns `a@2` on the next
`Next()` call.【F:range_test.go†L558-L586】

## 2. Directional gating for duplicate suppression relies on `forward` flipping

The duplicate/tombstone filter also hinges on `r.forward` toggling between
`true` and `false` to decide whether `allowAnchorOnNext` should allow a cached
crossing anchor to surface again. Because `consumePeekedReverse` forces
`r.forward = false` while `lastKey` is updated before the tombstone check, the
state machine can reject the deletion but still consider a different iterator's
older value as a legitimate direction change. This matches the behaviour seen in
`TestRangeIterator_DescendingSkipsTombstonedKey`, where the `a@2` payload is
accepted immediately after a tombstone despite the descending scan never moving
forward relative to the requested order.【F:range.go†L152-L181】【F:range_test.go†L558-L586】
