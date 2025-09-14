# Test Naming and Table-Driven Template

This document defines the naming pattern and table-driven structure used across unit and integration tests.

## Naming Format
Tests follow the `Test<Subject>_<Behavior>` format. Subtests describe scenarios in natural language.

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

## Table-Driven Template
When a test covers multiple cases, structure it using a table of test cases and `t.Run` for subtests.

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
