# Plan
- Add an atomic `globalTxnID` to uniquely name transaction logs.
- Refactor `transactionManager.begin` to accept a `FileSystem` and create `txn-<id>.log` in the database directory, copying existing data.
- `commit` syncs the shadow log before renaming it to the target path; `rollback` removes the shadow log.

```go
var globalTxnID atomic.Uint64

func (tm *transactionManager) begin(fs *FileSystem) (*transaction, error) {
    id := globalTxnID.Add(1)
    shadowPath := filepath.Join(filepath.Dir(fs.Path()), fmt.Sprintf("txn-%d.log", id))
    shadowFS, err := OpenFS(context.Background(), shadowPath)
    if err != nil {
        return nil, err
    }
    if src, err := os.Open(fs.Path()); err == nil {
        if _, err := io.Copy(shadowFS, src); err != nil {
            src.Close()
            shadowFS.Close()
            os.Remove(shadowPath)
            return nil, err
        }
        if err := src.Close(); err != nil {
            shadowFS.Close()
            os.Remove(shadowPath)
            return nil, err
        }
    } else if !errors.Is(err, os.ErrNotExist) {
        shadowFS.Close()
        os.Remove(shadowPath)
        return nil, err
    }
    return &transaction{id: id, log: shadowFS, target: fs, state: "active"}, nil
}

func (t *transaction) commit(ctx context.Context) error {
    if err := t.log.Sync(); err != nil {
        return err
    }
    if err := t.log.Close(); err != nil {
        return err
    }
    if err := os.Rename(t.log.Path(), t.target.Path()); err != nil {
        return err
    }
    if err := t.target.Close(); err != nil && !errors.Is(err, ErrFileNotOpened) {
        return err
    }
    return t.target.OpenExisting(ctx)
}
```
