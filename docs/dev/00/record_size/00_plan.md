# Record Size Encoding & Iterator Overhaul

1. **Thread record byte-length through the encoding layer.**
   - Extend `CalOnDiskSize` and the writer to account for the new trailer, emit the trailer after the checksum, and have the reader validate it so we can retro-derive the previous record's start without rescanning. Update any helpers that depend on the serialized footprint.
   - Surface the parsed byte-length (including header, payload, checksum, and trailer) so callers don't need to recompute it. This will require reshaping `readRecord`'s signature and plumbing the value through call sites.
   - **Complexity:** 8/10 &mdash; see the dedicated deep-dive in [`01_record_encoding_plan.md`](./01_record_encoding_plan.md).
   ```go
   func CalOnDiskSize(r Record) int {
       payload := len(r.GetKey()) + internalKeySuffixLen + len(r.GetValue())
       return 3*mdByteSize /* lens */ + payload + checksumSize + mdByteSize /* trailer */
   }

   func readRecord(storage io.Reader) (Record, int, error) {
       start := newMeteredReader(storage)
       // ... existing length reads ...
       checksum := consumeChecksum(start)
       size := start.BytesRead()
       trailer := readUint64(start)
       if int(trailer) != size {
           return nil, 0, fmt.Errorf("record size mismatch: got %d expect %d", trailer, size)
       }
       return RecordImpl{Key: userKey, Value: valueBytes, SequenceNumber: seq, Type: typ}, size, nil
   }

   func writeRecord(tx *transaction, record Record) error {
       // ... write header/payload/checksum ...
       return writeNumber(tx, uint64(CalOnDiskSize(record)))
   }
   ```

2. **Introduce a lightweight reader wrapper that can rewind via trailing sizes.**
   - Implement a helper (e.g., `recordReader`) that keeps track of the current offset and provides `PrevOffset()` by subtracting the recorded length from the current offset.
   - Update `offsetReader` usages to leverage the new metadata without allocating stacks, ensuring the helper can rehydrate the previous record by reopening a reader and reading the trailer.
   - **Complexity:** 6/10.
   ```go
   type recordReader struct {
       fs     *FileSystem
       offset int64
   }

   func (rr *recordReader) PrevOffset() (int64, error) {
       if rr.offset == 0 {
           return 0, io.EOF
       }
       buf := make([]byte, mdByteSize)
       if _, err := rr.fs.ReadAt(buf, rr.offset-mdByteSize); err != nil {
           return 0, err
       }
       size := int64(byteOrder.Uint64(buf))
       return rr.offset - size, nil
   }
   ```

3. **Refactor SSTable iterators to exploit record sizes instead of the offset stack.**
   - Replace `offsetStack` usages in `sstableIterator` and `sstableIRange` with on-demand calls to the new rewind helper, keeping the iterator state minimal while preserving O(1) `HasPrev` checks via cached offsets.
   - Ensure `Next`/`Prev` reuse the shared `readRecord` signature to obtain both the `Record` and its serialized size, updating the iterator's internal offset as they traverse.
   - **Complexity:** 7/10.
   ```go
   func (s *sstableIterator) Next() (Record, error) {
       reader := newOffsetReader(s.FileSystem, s.offset)
       rec, size, err := readRecord(reader)
       if err != nil {
           return nil, err
       }
       s.lastSize = int64(size)
       s.offset = reader.Offset()
       return rec, nil
   }

   func (s *sstableIterator) Prev() (Record, error) {
       if s.lastSize == 0 {
           return nil, EOI
       }
       start := s.offset - s.lastSize
       reader := newOffsetReader(s.FileSystem, start)
       rec, size, err := readRecord(reader)
       if err != nil {
           return nil, err
       }
       s.offset = start
       s.lastSize = int64(size)
       return rec, nil
   }
   ```

4. **Align builders, footers, and tests with the new layout.**
   - Update the SSTable builder's offset tracking, footer validation, and sparse index generation so `offset += CalOnDiskSize(rec)` remains accurate with the trailer; refresh fixtures and test helpers that assume the previous size.
   - Extend iterator and IO tests to assert round-tripping with the trailer, add regression coverage for `Prev` without the stack, and adjust any binary compatibility checks.
   - **Complexity:** 5/10.
   - **Status:** Completed &mdash; builder smoke tests now re-read trailer-sized records straight off disk, footers continue to gate sparse index placement, and iterator regressions cover `Prev`/`HasPrev` after eliminating the offset history knob.
   ```go
   func TestSSTableIterator_PrevWithoutStack(t *testing.T) {
       it, err := sst.Iterator()
       require.NoError(t, err)
       rec1, _ := it.Next()
       rec2, _ := it.Next()
       prev, _ := it.Prev()
       assert.Equal(t, rec2.GetKey(), prev.GetKey())
   }
   ```
