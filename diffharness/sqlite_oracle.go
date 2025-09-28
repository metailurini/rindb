package diffharness

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

// SQLiteOracle is a SQLite-backed reference implementation used by the
// diffharness. It stores every mutation with an explicit sequence number
// so reads can be performed at past snapshots.
type SQLiteOracle struct {
	db *sql.DB
	tx *sql.Tx
}

// OpenSQLiteOracle opens or creates a SQLite database at the given path and
// ensures the necessary schema exists.
func OpenSQLiteOracle(path string) (*SQLiteOracle, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	const schema = `
CREATE TABLE IF NOT EXISTS kv (
        k   BLOB    NOT NULL,
        seq INTEGER NOT NULL,
        v   BLOB,
        del INTEGER NOT NULL,
        PRIMARY KEY(k, seq)
);
`
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLiteOracle{db: db}, nil
}

func (o *SQLiteOracle) Begin(ctx context.Context) error {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	o.tx = tx
	return nil
}

func (o *SQLiteOracle) Commit(ctx context.Context) error {
	if o.tx == nil {
		return nil
	}
	err := o.tx.Commit()
	o.tx = nil
	return err
}

func (o *SQLiteOracle) Rollback(ctx context.Context) error {
	if o.tx == nil {
		return nil
	}
	err := o.tx.Rollback()
	o.tx = nil
	return err
}

func (o *SQLiteOracle) exec(query string, args ...any) error {
	if o.tx != nil {
		_, err := o.tx.Exec(query, args...)
		return err
	}
	_, err := o.db.Exec(query, args...)
	return err
}

// PutWithSeq inserts or replaces a value at the given sequence number.
func (o *SQLiteOracle) PutWithSeq(k, v []byte, seq uint64) error {
	return o.exec(`INSERT OR IGNORE INTO kv (k, seq, v, del) VALUES (?, ?, ?, 0)`, k, seq, v)
}

// DelWithSeq records a tombstone for the key at the provided sequence number.
func (o *SQLiteOracle) DelWithSeq(k []byte, seq uint64) error {
	return o.exec(`INSERT OR IGNORE INTO kv (k, seq, v, del) VALUES (?, ?, NULL, 1)`, k, seq)
}

// GetWithSeq retrieves the latest value for k at or before the snapshot
// sequence. It returns (nil, false, nil) if the key does not exist at the
// snapshot.
func (o *SQLiteOracle) GetWithSeq(k []byte, snapshot uint64) ([]byte, bool, error) {
	row := o.db.QueryRow(`SELECT v, del FROM kv WHERE k = ? AND seq <= ? ORDER BY seq DESC LIMIT 1`, k, snapshot)
	var v []byte
	var del int
	if err := row.Scan(&v, &del); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if del != 0 {
		return nil, false, nil
	}
	return v, true, nil
}

// RangeWithSeq returns all key/value pairs within [lo, hi] at the provided
// snapshot sequence, up to the specified limit, ordered according to the
// supplied direction. Keys deleted at the snapshot are omitted.
func (o *SQLiteOracle) RangeWithSeq(lo, hi []byte, order RangeOrder, snapshot uint64, limit int) ([]KV, error) {
	dir := "ASC"
	if order == RangeDesc {
		dir = "DESC"
	}
	query := fmt.Sprintf(`
SELECT kv.k, kv.v
FROM kv
JOIN (
        SELECT k, MAX(seq) AS mseq
        FROM kv
        WHERE k >= ? AND k <= ? AND seq <= ?
        GROUP BY k
) latest ON kv.k = latest.k AND kv.seq = latest.mseq
WHERE kv.del = 0
ORDER BY kv.k %s
LIMIT ?
`, dir)
	rows, err := o.db.Query(query, lo, hi, snapshot, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []KV
	for rows.Next() {
		var kv KV
		if err := rows.Scan(&kv.K, &kv.V); err != nil {
			return nil, err
		}
		res = append(res, kv)
	}
	return res, rows.Err()
}

// Close closes the underlying database.
func (o *SQLiteOracle) Close() error { return o.db.Close() }

// NewSnapshot records the current maximum sequence number and tracks it as an active snapshot.
func (o *SQLiteOracle) NewSnapshot(ctx context.Context) (uint64, error) {
	row := o.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM kv`)
	var seq uint64
	if err := row.Scan(&seq); err != nil {
		return 0, err
	}
	return seq, nil
}

// ReleaseSnapshot forgets about a previously created snapshot.
func (o *SQLiteOracle) ReleaseSnapshot(ctx context.Context, seq uint64) error {
	return nil
}
