package rindb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
)

// transactionManager manages transactions with a mutex for safe creation.
type transactionManager struct {
	mu         sync.Mutex
	activeTxns map[*transaction]struct{}
}

// newTransactionManager creates a new transactionManager.
func newTransactionManager() *transactionManager {
	return &transactionManager{
		activeTxns: make(map[*transaction]struct{}),
	}
}

// Begin starts a new transaction, ensuring thread-safe creation.
func (tm *transactionManager) begin() *transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock() // Unlock after creating the transaction

	txn := &transaction{
		buffer:  bytes.NewBuffer(nil),
		manager: tm,
		state:   "active",
	}
	tm.activeTxns[txn] = struct{}{}
	return txn
}

// transaction represents a single transaction with its own state.
type transaction struct {
	buffer  *bytes.Buffer
	manager *transactionManager // Reference to the manager.
	mu      sync.Mutex          // Per-transaction lock for thread safety.
	state   string              // "active", "committed", or "rolledback".
}

// write writes data to the transaction buffer, ensuring thread safety.
func (t *transaction) write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "active" {
		return 0, errors.New("transaction is not active")
	}
	return t.buffer.Write(p)
}

// commit writes the buffered data to the provided writer and marks the transaction as committed.
func (t *transaction) commit(ctx context.Context, w io.Writer) error {
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
	info(ctx, "Transaction committed successfully")
	return nil
}

// rollback discards the transaction's buffer and marks it as rolled back.
func (t *transaction) rollback(ctx context.Context) error {
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
	info(ctx, "Transaction rolled back successfully")
	return nil
}

// isActive checks if the transaction is still active (optional utility method).
func (t *transaction) isActive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == "active"
}
