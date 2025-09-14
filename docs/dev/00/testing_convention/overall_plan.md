# Testing Convention Overhaul Plan

1. **Define naming format**
   - Plan: Document a consistent `Test<Subject>_<Behavior>` pattern for unit and integration tests; subtests should describe scenarios in natural language.
   ```go
   // Unit test example
   func TestMemtable_Get(t *testing.T) {
       t.Run("found", func(t *testing.T) {
           // ...
       })
   }

   // Integration test example
   //go:build integration
   func TestCompaction_PreservesTombstone(t *testing.T) {
       // ...
   }
   ```
   - Complexity: 2/10
   - Dedicated plan file: No

2. **Establish table-driven template**
   - Plan: Provide a standard table-driven template to cover multiple cases with `t.Run`.
   ```go
   func TestRecord_Encode(t *testing.T) {
       tests := []struct {
           name string
           rec  Record
           want []byte
       }{
           {"value record", newRecord(Bytes("k"), Bytes("v"), 1), []byte{0x01}},
           {"tombstone", newDeletion(Bytes("k"), 2), []byte{0x00}},
       }
       for _, tt := range tests {
           tt := tt
           t.Run(tt.name, func(t *testing.T) {
               got := Encode(tt.rec)
               assert.Equal(t, tt.want, got)
           })
       }
   }
   ```
   - Complexity: 4/10
   - Dedicated plan file: No

3. **Refactor unit tests**
   - Plan: Rename existing unit test functions and convert ad-hoc checks into table-driven subtests.
   ```go
   // before
   func TestMemtable_Basic(t *testing.T) {
       // ...
   }

   // after
   func TestMemtable_BasicOperations(t *testing.T) {
       cases := []struct{name, key, val string}{{"put/get", "k1", "v1"}}
       for _, tt := range cases {
           t.Run(tt.name, func(t *testing.T) {
               // ...
           })
       }
   }
   ```
   - Complexity: 8/10
   - Dedicated plan file: Yes (unit_tests_refactor_plan.md)

4. **Refactor integration tests and enforce naming check**
   - Plan: Align integration test names with the same pattern and add a dedicated tool to flag misnamed tests.
   ```go
   //go:build integration
   func TestRangeQuery_ReturnsOrderedKeys(t *testing.T) {
       // ...
   }

   // run during `make check`
   $ go run ./tool/testname ./integration
   ```
   - Complexity: 7/10
   - Dedicated plan file: Yes (integration_tests_refactor_plan.md)
