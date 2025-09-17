# Detailed Plan: Record Encoding Trailer

## Goals
- Append an 8-byte length trailer to every serialized record so iterators can jump backwards without auxiliary stacks.
- Preserve checksum validation semantics while ensuring the trailer can't be out of sync with the actual bytes consumed.
- Maintain compatibility with existing builders/tests by updating helpers and fixtures atomically.

## Tasks
1. **Augment size accounting.** Revise `CalOnDiskSize` to include the trailer, and audit any other arithmetic that assumed `CalOnDiskSize` only covered header+payload+checksum.
2. **Refactor `readRecord`.**
   - Introduce a small `meteredReader` (wrapping `io.Reader`) to count consumed bytes.
   - Parse the existing header/value/checksum sequence, then read the trailing size.
   - Return both the materialized `Record` and the measured size so callers avoid double-reading.
3. **Update `writeRecord`.** After writing the checksum, emit the trailer using the new size accounting.
4. **Call-site updates.** Touch all places that call `readRecord` so they handle the new `(Record, int, error)` signature and store the size when needed.
5. **Validation tests.** Add/extend tests (likely in `io_test.go` and `sstable_iteration_test.go`) to cover malformed trailers and ensure `Prev` seeks correctly using the trailer.

## Open Questions & Follow-ups
- Decide whether legacy SSTables (without trailers) must remain readable; if yes, gate the trailer behind a version byte in the footer or detect via feature flag.
- Confirm whether WAL or memtable serialization shares the same encoding; adjust the scope if other components rely on the record layout.

## Definition of Done
- Builders emit the new trailer and iterators no longer depend on `offsetStack`.
- All unit/integration tests pass, and new tests cover regression scenarios for mismatched trailers and backward iteration.
