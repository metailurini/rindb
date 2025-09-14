# Integration Tests Refactor Plan

1. **Rename integration tests**
   - Plan: Apply `Test<Subject>_<Behavior>` and include `//go:build integration` tags.
   ```go
   //go:build integration
   // old
   func TestTableCacheMissAfterGetFollowingCompaction(t *testing.T) {}
   // new
   func TestTableCache_GetAfterCompactionMiss(t *testing.T) {}
   ```
   - Complexity: 6/10

2. **Normalize setup/teardown**
   - Plan: Introduce helper constructors to reduce duplicated environment setup.
   ```go
   func newIntegrationDB(t *testing.T) *DB {
       t.Helper()
       dir := t.TempDir()
       db, err := Open(dir, WithWAL(), WithMemTableSize(1024))
       require.NoError(t, err)
       return db
   }
   ```
   - Complexity: 4/10

3. **Adopt table-driven subtests**
   - Plan: Restructure tests that cover multiple scenarios into table-driven format.
   ```go
   func TestRangeQuery_ReturnsOrderedKeys(t *testing.T) {
       cases := []struct{
           name string
           keys []string
       }{
           {"ascending", []string{"a","b"}},
           {"descending", []string{"b","a"}},
       }
       for _, tt := range cases {
           t.Run(tt.name, func(t *testing.T) {
               // ...
           })
       }
   }
   ```
   - Complexity: 7/10

4. **Add naming enforcement test**
   - Plan: Implement a test that fails when functions don't match regex `^Test[A-Za-z0-9]+_[A-Za-z0-9]+$`.
   ```go
   var testNameRe = regexp.MustCompile(`^Test[A-Z][a-zA-Z0-9]*_[a-zA-Z0-9_]+$`)

   func TestNamingConventions(t *testing.T) {
       for _, name := range testNamesFromPackage(t) {
           if !testNameRe.MatchString(name) {
               t.Errorf("%s does not follow Test<Subject>_<Behavior>", name)
           }
       }
   }
   ```
   - Complexity: 8/10

5. **Run smoke and full suites**
   - Plan: Execute `make test` and `make test-integration-smoke` to validate changes.
   ```sh
   make test
   make test-integration-smoke
   ```
   - Complexity: 5/10
