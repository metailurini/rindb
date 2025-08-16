package rindb

import (
	"bytes"
	"context"
	"sync"
	"testing"
)

// TestNewTransactionManager verifies that NewTransactionManager initializes correctly.
func TestNewTransactionManager(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	if tm == nil {
		t.Fatal("NewTransactionManager returned nil")
	}
}

// TestBegin checks that Begin creates a valid transaction.
func TestBegin(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	txn := tm.Begin()

	if txn == nil {
		t.Fatal("Begin returned nil")
	}
	if txn.buffer == nil {
		t.Fatal("Transaction buffer is nil")
	}
	if txn.state != "active" {
		t.Errorf("Expected state 'active', got %q", txn.state)
	}
	if !txn.IsActive() {
		t.Error("Expected transaction to be active")
	}
}

// TestWrite verifies that Write works correctly on an active transaction.
func TestWrite(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	txn := tm.Begin()

	n, err := txn.Write([]byte("test data"))
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != 9 {
		t.Errorf("Expected 9 bytes written, got %d", n)
	}
	if txn.buffer.String() != "test data" {
		t.Errorf("Expected buffer to contain 'test data', got %q", txn.buffer.String())
	}
}

// TestWriteAfterCommit ensures Write fails after committing.
func TestWriteAfterCommit(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	txn := tm.Begin()

	txn.Write([]byte("data"))
	var buf bytes.Buffer
	if err := txn.Commit(&buf); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	_, err := txn.Write([]byte("more"))
	if err == nil || err.Error() != "transaction is not active" {
		t.Errorf("Expected error 'transaction is not active', got %v", err)
	}
}

// TestCommit verifies that Commit writes data correctly and updates state.
func TestCommit(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	txn := tm.Begin()

	txn.Write([]byte("commit me"))
	var buf bytes.Buffer
	err := txn.Commit(&buf)
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
	if buf.String() != "commit me" {
		t.Errorf("Expected 'commit me' in buffer, got %q", buf.String())
	}
	if txn.state != "committed" {
		t.Errorf("Expected state 'committed', got %q", txn.state)
	}
	if txn.buffer.Len() != 0 {
		t.Errorf("Expected buffer to be empty after commit, got %d bytes", txn.buffer.Len())
	}
}

// TestCommitAfterCommit ensures a second Commit fails.
func TestCommitAfterCommit(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	txn := tm.Begin()

	txn.Write([]byte("data"))
	var buf bytes.Buffer
	txn.Commit(&buf)

	err := txn.Commit(&buf)
	if err == nil || err.Error() != "transaction is not active" {
		t.Errorf("Expected error 'transaction is not active', got %v", err)
	}
}

// TestRollback verifies that Rollback resets the buffer and updates state.
func TestRollback(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	txn := tm.Begin()

	txn.Write([]byte("rollback me"))
	err := txn.Rollback()
	if err != nil {
		t.Errorf("Rollback failed: %v", err)
	}
	if txn.buffer.Len() != 0 {
		t.Errorf("Expected buffer to be empty after rollback, got %d bytes", txn.buffer.Len())
	}
	if txn.state != "rolledback" {
		t.Errorf("Expected state 'rolledback', got %q", txn.state)
	}
}

// TestRollbackAfterRollback ensures a second Rollback fails.
func TestRollbackAfterRollback(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	txn := tm.Begin()

	txn.Write([]byte("data"))
	txn.Rollback()

	err := txn.Rollback()
	if err == nil || err.Error() != "transaction is not active" {
		t.Errorf("Expected error 'transaction is not active', got %v", err)
	}
}

// TestConcurrentBegin ensures multiple goroutines can call Begin without blocking.
func TestConcurrentBegin(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	var wg sync.WaitGroup
	const numGoroutines = 10

	wg.Add(numGoroutines)
	txns := make([]*Transaction, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			txns[idx] = tm.Begin()
		}(i)
	}

	wg.Wait()
	for i, txn := range txns {
		if txn == nil {
			t.Errorf("Transaction %d is nil", i)
		}
		if !txn.IsActive() {
			t.Errorf("Transaction %d is not active", i)
		}
	}
}

// TestConcurrentWrite ensures a single transaction is thread-safe for writes.
func TestConcurrentWrite(t *testing.T) {
	tm := NewTransactionManager(context.Background())
	txn := tm.Begin()
	var wg sync.WaitGroup
	const numWrites = 100

	wg.Add(numWrites)
	for i := 0; i < numWrites; i++ {
		go func(n int) {
			defer wg.Done()
			data := []byte{byte(n)}
			_, err := txn.Write(data)
			if err != nil {
				t.Errorf("Write failed in goroutine %d: %v", n, err)
			}
		}(i)
	}

	wg.Wait()
	if txn.buffer.Len() != numWrites {
		t.Errorf("Expected %d bytes in buffer, got %d", numWrites, txn.buffer.Len())
	}
}
