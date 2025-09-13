# Skiplist backward links maintenance plan

This document expands step 2 of the reverse range scanning plan and explains how level-0 `backward` links remain valid during mutations.

1. **Update `Put`**
   - When inserting a node, set its `backward` pointer to the previous level-0 node and update the subsequent node's `backward` pointer to point to the new node.
   - Handle concurrent inserts by holding the existing memtable lock; iterators rely on the immutability of nodes once linked.
   - Example:
     ```go
     func (m *memtable) Put(k, v []byte) {
         pred, succ := findSplice(k)
         n := newNode(k, v)
         n.backward = pred
         if succ != nil { succ.backward = n }
         insertBetween(pred, n, succ)
     }
     ```
2. **Update `Remove`**
   - Splice the target from level-0 forward and backward lists.
   - Rewire the successor's `backward` pointer to the predecessor so iterators can continue stepping backward.
   - Example:
     ```go
     func (m *memtable) Remove(k []byte) {
         n := findNode(k)
         pred, succ := n.backward, n.forwards[0]
         if succ != nil { succ.backward = pred }
         unlink(n)
     }
     ```
3. **Concurrency assumptions**
   - Memtable mutations are serialized with existing locking; readers may traverse concurrently because nodes are never modified after unlink.
4. **Testing**
   - Add unit tests that interleave `Put` and `Remove` with forward and reverse iteration, verifying order and absence of stale pointers.
   - Include cases that insert and delete around the iterator's current position.
