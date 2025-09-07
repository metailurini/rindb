# Plan
- Add an atomic `globalTxnID` to uniquely name transaction logs.
- Refactor `transactionManager.begin` to accept a `FileSystem` and create `txn-<id>.log` in the database directory, copying existing data.
- Modify `commit` and `rollback` to rename or remove the shadow log respectively and reopen the target file.

```go
var globalTxnID atomic.Uint64

func (tm *transactionManager) begin(fs *FileSystem) (*transaction, error) {
    id := globalTxnID.Add(1)
    shadowPath := filepath.Join(filepath.Dir(fs.Path()), fmt.Sprintf("txn-%d.log", id))
    shadowFS, _ := OpenFS(context.Background(), shadowPath)
    io.Copy(shadowFS, mustOpen(fs.Path()))
    return &transaction{id: id, log: shadowFS, target: fs, state: "active"}, nil
}

func (t *transaction) commit(ctx context.Context) error {
    t.log.Close()
    os.Rename(t.log.Path(), t.target.Path())
    return t.target.OpenExisting(ctx)
}
```
