package fuzzing

import (
	"database/sql"
	"errors"

	_ "modernc.org/sqlite"
)

// SQLiteOracle is a SQLite-backed reference implementation used by the
// fuzzing harness. It stores every mutation with an explicit sequence number
// so reads can be performed at past snapshots.
type SQLiteOracle struct{ db *sql.DB }

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

// PutWithSeq inserts or replaces a value at the given sequence number.
func (o *SQLiteOracle) PutWithSeq(k, v []byte, seq uint64) error {
	_, err := o.db.Exec(`INSERT INTO kv (k, seq, v, del) VALUES (?, ?, ?, 0)`, k, seq, v)
	return err
}

// DelWithSeq records a tombstone for the key at the provided sequence number.
func (o *SQLiteOracle) DelWithSeq(k []byte, seq uint64) error {
	_, err := o.db.Exec(`INSERT INTO kv (k, seq, v, del) VALUES (?, ?, NULL, 1)`, k, seq)
	return err
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

// RangeWithSeq returns all key/value pairs within [lo, hi) at the provided
// snapshot sequence, up to the specified limit. Keys deleted at the snapshot
// are omitted.
func (o *SQLiteOracle) RangeWithSeq(lo, hi []byte, snapshot uint64, limit int) ([]KV, error) {
	rows, err := o.db.Query(`
SELECT kv.k, kv.v
FROM kv
JOIN (
        SELECT k, MAX(seq) AS mseq
        FROM kv
        WHERE k >= ? AND k < ? AND seq <= ?
        GROUP BY k
) latest ON kv.k = latest.k AND kv.seq = latest.mseq
WHERE kv.del = 0
ORDER BY kv.k
LIMIT ?
`, lo, hi, snapshot, limit)
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
