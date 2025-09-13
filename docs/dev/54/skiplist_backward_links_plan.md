# Skiplist backward links maintenance plan

This document expands step 2 of the reverse range scanning plan and explains how level-0 `backward` links remain valid during mutations.

1. **Update `Put`**
   - When inserting a node, set its `backward` pointer to the previous level-0 node and update the next node's pointer if needed.
   - Handle concurrent inserts by holding the existing memtable lock; iterators rely on the immutability of nodes once linked.
2. **Update `Remove`**
   - Splice the target from level-0 forward and backward lists.
   - Rewire the successor's `backward` pointer to the predecessor so iterators can continue stepping backward.
3. **Concurrency assumptions**
   - Memtable mutations are serialized with existing locking; readers may traverse concurrently because nodes are never modified after unlink.
4. **Testing**
   - Add unit tests that interleave `Put` and `Remove` with forward and reverse iteration, verifying order and absence of stale pointers.
   - Include cases that insert and delete around the iterator's current position.
