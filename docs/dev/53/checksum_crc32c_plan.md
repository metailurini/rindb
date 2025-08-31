# CRC-32C Migration Plan

## Objective
Switch checksum calculations from streaming CRC-32 IEEE (`crc32.NewIEEE()`) to one-shot CRC-32C Castagnoli.

## Plan
1. **Introduce CRC-32C Table**
   - Create a package-level lookup table so every checksum call can reuse it.
   ```go
   var crc32cTable = crc32.MakeTable(crc32.Castagnoli)
   ```

2. **Replace Streaming Hashers**
   - Convert existing code that constructs a `crc32.NewIEEE()` hasher and writes to it in chunks.
   - Instead, compute the checksum directly using `crc32.Update` with the CRC-32C table.
   ```go
   sum := crc32.Update(0, crc32cTable, buf)
   ```

3. **Update Write Path**
   - In modules like `io.go`, remove incremental writes and pass slices to a helper that folds them with `crc32.Update`.
   - Return the resulting checksum as `uint32` without keeping a hasher state.
   ```go
   func checksum(parts ...[]byte) uint32 {
       var sum uint32
       for _, p := range parts {
           sum = crc32.Update(sum, crc32cTable, p)
       }
       return sum
   }
   ```

4. **Adjust Tests**
   - Rewrite tests that previously fed data to the streaming hasher.
   - Call the new helper with one or multiple slices and assert against precomputed CRC-32C values.
   ```go
   expected := uint32(0x8a9136aa)
   actual := checksum([]byte("foo"), []byte("bar"))
   require.Equal(t, expected, actual)
   ```

5. **Verify Build & Tests**
   - Run `make check`, `make test`, and `make test-integration-smoke` to ensure the code compiles and behaves correctly.

## Rollout Considerations
- Review any interoperability requirements; CRC-32C differs from IEEE, so persisted data may need versioning.
- Document the change so users know new checksums are CRC-32C.
