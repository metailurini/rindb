package rindb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
)

// TransactionManager manages transactions with a mutex for safe creation.
type TransactionManager struct {
	mu         sync.Mutex
	activeTxns map[*Transaction]struct{}
}

// NewTransactionManager creates a new TransactionManager.
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		activeTxns: make(map[*Transaction]struct{}),
	}
}

// Begin starts a new transaction, ensuring thread-safe creation.
func (tm *TransactionManager) Begin() *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock() // Unlock after creating the transaction

	txn := &Transaction{
		buffer:  bytes.NewBuffer(nil),
		manager: tm,
		state:   "active",
	}
	tm.activeTxns[txn] = struct{}{}
	return txn
}

// Transaction represents a single transaction with its own state.
type Transaction struct {
	buffer  *bytes.Buffer
	manager *TransactionManager // Reference to the manager.
	mu      sync.Mutex          // Per-transaction lock for thread safety.
	state   string              // "active", "committed", or "rolledback".
}

// Write writes data to the transaction buffer, ensuring thread safety.
func (t *Transaction) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "active" {
		return 0, errors.New("transaction is not active")
	}
	return t.buffer.Write(p)
}

// Commit writes the buffered data to the provided writer and marks the transaction as committed.
func (t *Transaction) Commit(ctx context.Context, w io.Writer) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "active" {
		return errors.New("transaction is not active")
	}

	_, err := w.Write(t.buffer.Bytes())
	if err != nil {
		return err
	}

	t.state = "committed"
	t.buffer.Reset() // Clear the buffer after committing.

	t.manager.mu.Lock()
	delete(t.manager.activeTxns, t)
	t.manager.mu.Unlock()
	INFO(ctx, "Transaction committed successfully")
	return nil
}

// Rollback discards the transaction's buffer and marks it as rolled back.
func (t *Transaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "active" {
		return errors.New("transaction is not active")
	}

	t.buffer.Reset()
	t.state = "rolledback"

	t.manager.mu.Lock()
	delete(t.manager.activeTxns, t)
	t.manager.mu.Unlock()
	INFO(ctx, "Transaction rolled back successfully")
	return nil
}

// IsActive checks if the transaction is still active (optional utility method).
func (t *Transaction) IsActive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == "active"
}
