# Unit Tests Refactor Plan

1. **Inventory and rename tests**
   - Plan: List all unit test functions, rename to `Test<Subject>_<Behavior>`.
   ```go
   // old
   func TestMemtable_Basic(t *testing.T) {}
   // new
   func TestMemtable_BasicOperations(t *testing.T) {}
   ```
   - Complexity: 5/10

2. **Adopt table-driven structure**
   - Plan: Convert ad-hoc checks to table-driven subtests using `t.Run`.
   ```go
   func TestTableCache_Load(t *testing.T) {
       cases := []struct{
           name string
           key  string
           want bool
       }{
           {"hit", "k1", true},
           {"miss", "k2", false},
       }
       for _, tt := range cases {
           t.Run(tt.name, func(t *testing.T) {
               got := cache.Load(tt.key)
               require.Equal(t, tt.want, got)
           })
       }
   }
   ```
   - Complexity: 8/10

3. **Extract shared helpers**
   - Plan: Move repeated setup/teardown into helper functions with `t.Helper()`.
   ```go
   func newTestDB(t *testing.T) *DB {
       t.Helper()
       dir := t.TempDir()
       db, err := Open(dir)
       require.NoError(t, err)
       return db
   }
   ```
   - Complexity: 4/10

4. **Enforce linting and run tests**
   - Plan: Ensure `make check` and `make test` pass after refactors.
   ```sh
   make check
   make test
   ```
   - Complexity: 3/10
