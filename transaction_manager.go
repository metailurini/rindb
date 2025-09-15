package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	globalTxnID    atomic.Uint64
	osOpen         = os.Open
	ioCopy         = io.Copy
	osRename       = os.Rename
	osRemove       = os.Remove
	fsSync         = (*FileSystem).Sync
	fsClose        = (*FileSystem).Close
	fsOpenExisting = (*FileSystem).OpenExisting
	fsWrite        = (*FileSystem).Write
)

// transactionManager manages transactions with a mutex for safe creation.
type transactionManager struct {
	mu         sync.Mutex
	activeTxns map[*transaction]struct{}
	log        scopedLogger
}

// newTransactionManager creates a new transactionManager.
func newTransactionManager(log scopedLogger) *transactionManager {
	return &transactionManager{
		activeTxns: make(map[*transaction]struct{}),
		log:        log,
	}
}

// begin starts a new transaction for the provided FileSystem.
func (tm *transactionManager) begin(ctx context.Context, fs *FileSystem) (*transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	id := globalTxnID.Add(1)

	// copy current data to shadow log
	path := fs.Path()
	if err := fs.Close(); err != nil && !errors.Is(err, ErrFileNotOpened) {
		return nil, err
	}
	dir := filepath.Dir(path)
	shadowPath := filepath.Join(dir, fmt.Sprintf("txn-%d.log", id))

	shadowFS, err := OpenFS(ctx, shadowPath)
	if err != nil {
		return nil, err
	}

	if src, err := osOpen(path); err == nil {
		if _, err := ioCopy(shadowFS, src); err != nil {
			_ = shadowFS.Close()
			_ = os.Remove(shadowPath)
			_ = src.Close()
			return nil, err
		}
		if err := src.Close(); err != nil {
			_ = shadowFS.Close()
			_ = os.Remove(shadowPath)
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = shadowFS.Close()
		_ = os.Remove(shadowPath)
		return nil, err
	}

	txn := &transaction{
		id:      id,
		log:     shadowFS,
		target:  fs,
		manager: tm,
		state:   "active",
		written: 0,
	}
	tm.activeTxns[txn] = struct{}{}
	return txn, nil
}

// transaction represents a single transaction with its own state.
type transaction struct {
	id      uint64
	log     *FileSystem
	target  *FileSystem
	manager *transactionManager
	mu      sync.Mutex
	state   string
	written int64
}

// write writes data to the transaction log, ensuring thread safety.
func (t *transaction) write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "active" {
		return 0, errors.New("transaction is not active")
	}
	var total int
	for len(p) > 0 {
		n, err := fsWrite(t.log, p)
		total += n
		t.written += int64(n)
		if err != nil {
			return total, err
		}
		if n == len(p) {
			break
		}
		p = p[n:]
	}
	return total, nil
}

func (t *transaction) size() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.written
}

// commit renames the shadow log to the original file path.
func (t *transaction) commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "active" {
		return errors.New("transaction is not active")
	}

	if err := fsSync(t.log); err != nil {
		return err
	}
	if err := fsClose(t.log); err != nil {
		return err
	}
	if err := osRename(t.log.Path(), t.target.Path()); err != nil {
		return err
	}
	if err := fsClose(t.target); err != nil && !errors.Is(err, ErrFileNotOpened) {
		return err
	}
	if err := fsOpenExisting(t.target, ctx); err != nil {
		return err
	}

	t.state = "committed"
	t.manager.mu.Lock()
	delete(t.manager.activeTxns, t)
	t.manager.mu.Unlock()
	t.manager.log.info(ctx, "Transaction committed successfully")
	return nil
}

// rollback removes the shadow log and restores the original file.
func (t *transaction) rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "active" {
		return errors.New("transaction is not active")
	}

	shadowPath := t.log.Path()
	if err := fsClose(t.log); err != nil {
		return err
	}
	if err := osRemove(shadowPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove shadow log %q during rollback: %w", shadowPath, err)
	}

	t.state = "rolledback"
	t.manager.mu.Lock()
	delete(t.manager.activeTxns, t)
	t.manager.mu.Unlock()
	t.manager.log.info(ctx, "Transaction rolled back successfully")
	return nil
}

// isActive checks if the transaction is still active.
func (t *transaction) isActive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == "active"
}
