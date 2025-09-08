# RinDB Differential Test Harness (with Snapshots)

This doc gives you a ready-to-run framework to **continuously** compare your LSM engine (“RinDB”) against a **SQLite-based behavioral oracle**. It models MVCC visibility (sequence numbers + tombstones), supports **point reads, ranges, deletes, and snapshots**, and is designed for **metamorphic**, **crash/recovery**, and **fault-injection** testing. Copy/paste the snippets into your repo and extend.

---

## Goals

* **Detect correctness deltas** between RinDB and a SQLite oracle over very long runs.
* Exercise **snapshots**, **tombstones**, **compaction visibility**, and **range iteration**.
* Produce **reproducible** failure logs (seed + op log) and **shrinkable** repros.
* Run **forever** (or until error) with periodic invariant checks.

## Implementation Steps

1. Set up a harness and shared `Engine` interface that logs every operation and tracks snapshots with a global sequence counter.
2. Implement a SQLite-backed oracle that mirrors the `Engine` interface but accepts explicit sequence numbers for writes and reads.
3. Drive both engines with randomized `Put`, `Delete`, `Get`, and `Range` operations, comparing results and running invariant checks on each step.
4. Extend the harness with metamorphic configuration runs, crash/recovery hooks, and telemetry to expose long‑run correctness issues.

---

## Behavioral Model & Assumptions

* RinDB assigns a strictly **monotonic sequence** `seq` per *logical write* (Put/Delete).
* Visibility at a **snapshot S** = the latest version with `seq ≤ S`, where `val != NULL`.
* Delete = **tombstone** (no value; hides earlier versions at `seq ≤ S`).
* Range results are in **key ascending order**.

---

## SQLite Oracle: Schema & Queries

We model multiple versions per key; `NULL` value is a tombstone.

```sql
-- oracle_schema.sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA temp_store=MEMORY;

CREATE TABLE IF NOT EXISTS kv (
  user_key BLOB NOT NULL,
  seq      INTEGER NOT NULL,  -- RinDB global sequence at write time
  val      BLOB,              -- NULL == tombstone
  PRIMARY KEY (user_key, seq)
);

-- Current max sequence (helper view)
CREATE VIEW IF NOT EXISTS _max_seq AS
SELECT COALESCE(MAX(seq), 0) AS max_seq FROM kv;

-- Get visible value for a key at snapshot :snap
-- SELECT val FROM kv WHERE user_key=? AND seq<=:snap ORDER BY seq DESC LIMIT 1;

-- Range [lo, hi) at snapshot :snap
-- SELECT user_key, FIRST_VALUE(val) OVER (PARTITION BY user_key ORDER BY seq DESC) AS val
-- FROM kv
-- WHERE user_key >= :lo AND user_key < :hi AND seq <= :snap
-- QUALIFY val IS NOT NULL
-- GROUP BY user_key;
```

> Notes
>
> * We use `FIRST_VALUE ... ORDER BY seq DESC` to find the latest visible version per key.
> * If your SQLite build lacks window functions, substitute with a correlated subquery.

---

## Go Interfaces

```go
// engine.go — your DB + the oracle adapters share this shape
package diffharness

import "context"

type KV struct{ K, V []byte }

type Engine interface {
Put(ctx context.Context, k, v []byte) error // latest write; engine tags with seq internally
Delete(ctx context.Context, k []byte) error // tombstone
Get(ctx context.Context, k []byte, snapshot uint64) ([]byte, bool, error)
Range(ctx context.Context, lo, hi []byte, snapshot uint64, limit int) ([]KV, error)
// Optional: expose internal "now" for snapshot mapping; else the harness holds seq.
Close() error
}
```

---

## SQLite Oracle (Go Adapter)

The harness needs an oracle that mirrors the `Engine` interface but accepts
explicit sequence numbers. Implement at least the following helpers:

* `PutWithSeq(k, v, seq)` and `DelWithSeq(k, seq)` – append writes.
* `GetWithSeq(k, snap) ([]byte, bool, error)` – read latest visible value.
* `RangeWithSeq(lo, hi, snap, limit) ([]KV, error)` – range query at a snapshot.

```go
// sqlite_oracle.go
package diffharness

import (
"context"
"database/sql"
_ "modernc.org/sqlite"
)

type SQLiteOracle struct {
db *sql.DB
}

func OpenSQLiteOracle(path string) (*SQLiteOracle, error) {
db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
if err != nil { return nil, err }
_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS kv (user_key BLOB NOT NULL, seq INTEGER NOT NULL, val BLOB, PRIMARY KEY(user_key, seq));
`)
if err != nil { db.Close(); return nil, err }
return &SQLiteOracle{db: db}, nil
}

func (o *SQLiteOracle) Close() error { return o.db.Close() }

func (o *SQLiteOracle) PutWithSeq(k, v []byte, seq uint64) error {
_, err := o.db.Exec(`INSERT INTO kv(user_key, seq, val) VALUES(?, ?, ?)`, k, seq, v)
return err
}

func (o *SQLiteOracle) DelWithSeq(k []byte, seq uint64) error {
_, err := o.db.Exec(`INSERT INTO kv(user_key, seq, val) VALUES(?, ?, NULL)`, k, seq)
return err
}

// GetWithSeq returns the latest visible value for k at snapshot snap.
func (o *SQLiteOracle) GetWithSeq(k []byte, snap uint64) ([]byte, bool, error) {
row := o.db.QueryRow(`SELECT val FROM kv WHERE user_key=? AND seq<=? ORDER BY seq DESC LIMIT 1`, k, snap)
var val []byte
if err := row.Scan(&val); err != nil {
if err == sql.ErrNoRows { return nil, false, nil }
return nil, false, err
}
if val == nil { return nil, false, nil } // tombstone
return val, true, nil
}

// RangeWithSeq emits the first non-tombstoned version for each key in [lo,hi).
func (o *SQLiteOracle) RangeWithSeq(lo, hi []byte, snap uint64, limit int) ([]KV, error) {
q := `
WITH ranked_kv AS (
  SELECT
    user_key,
    val,
    ROW_NUMBER() OVER (PARTITION BY user_key ORDER BY seq DESC) AS rn
  FROM kv
  WHERE user_key >= ? AND user_key < ? AND seq <= ?
)
SELECT user_key, val
FROM ranked_kv
WHERE rn = 1 AND val IS NOT NULL
ORDER BY user_key
LIMIT ?;
`
rows, err := o.db.Query(q, lo, hi, snap, limit)
if err != nil { return nil, err }
defer rows.Close()
var out []KV
for rows.Next() {
var k, v []byte
if err := rows.Scan(&k, &v); err != nil { return nil, err }
out = append(out, KV{K: k, V: v})
}
return out, rows.Err()
}

// Optional helper to get current max seq stored in oracle (not required).
func (o *SQLiteOracle) MaxSeq(ctx context.Context) (uint64, error) {
row := o.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM kv`)
var s uint64
err := row.Scan(&s)
return s, err
}
```

---

## RinDB Adapter (Glue)

Implement these wrappers calling into your engine. If your engine exposes snapshots as handles, map **handle → uint64 sequence** (the harness keeps the global `seq` and passes it in).

```go
// rindb_adapter.go
package diffharness

type RinDB struct {
// your db handle(s)
}

func OpenRinDB(path string) (*RinDB, error) {
// open your DB
return &RinDB{}, nil
}

func (r *RinDB) Close() error { /* close */ return nil }

func (r *RinDB) Put(k, v []byte) error {
// your code
return nil
}

func (r *RinDB) Delete(k []byte) error {
// your code
return nil
}

func (r *RinDB) Get(k []byte, snapshot uint64) ([]byte, bool, error) {
// If your API has both Get(k) and GetAt/GetWithSeq variants, use the right one.
// Implement snapshot mapping as needed.
return nil, false, nil
}

func (r *RinDB) Range(lo, hi []byte, snapshot uint64, limit int) ([]KV, error) {
// call your range API respecting snapshot
return nil, nil
}
```

---

## Operation Model & Snapshots

**Operations**:

* `Put(k,v)` → increments global `seq`, writes to both DBs.
* `Del(k)` → increments `seq`, writes to both DBs.
* `Get(k, S)` → compare results at snapshot `S` (or latest, if S=0).
* `Range([lo,hi), S)` → compare lists.
* `Snap()` → records **current seq** into a snapshot pool for later reads.

**Snapshot rule**: `S = currentSeq` at time of creation.

---

## Fuzzer & Comparator (Go)

```go
// fuzzer.go
package diffharness

import (
"bytes"
cryptoRand "crypto/rand"
"encoding/json"
"errors"
"fmt"
"log"
"math/rand"
"os"
"time"
)

type OpKind uint8
const (
OpPut OpKind = iota
OpDel
OpGet
OpRange
OpSnap
)

type Op struct {
Kind     OpKind
K, V     []byte
Lo, Hi   []byte
SnapSeq  uint64
Limit    int
}

type Harness struct {
Seed int64

My   Engine
Ref  *SQLiteOracle

Seq        uint64   // global sequence for writes
Snapshots  []uint64 // pool of past snapshot seqs
LogFile    *os.File // JSONL repro log
}

type Cfg struct {
KeyLen       int
ValLenMin    int
ValLenMax    int
RangeMax     int
Weights      map[OpKind]int // probability weights
}

func NewHarness(my Engine, ref *SQLiteOracle, seed int64, logPath string) (*Harness, error) {
f, err := os.Create(logPath)
if err != nil { return nil, err }
return &Harness{My: my, Ref: ref, Seed: seed, LogFile: f}, nil
}

func (h *Harness) Close() error { return h.LogFile.Close() }

func randBytes(r *rand.Rand, n int) []byte {
b := make([]byte, n)
_, _ = r.Read(b)
return b
}
func randKey(r *rand.Rand, n int) []byte {
// bias toward some hot prefixes to create hot-spot distributions if desired
return randBytes(r, n)
}

func (h *Harness) genOp(r *rand.Rand, cfg Cfg) Op {
// Weighted choice
sum := 0
for _, w := range cfg.Weights { sum += w }
x := r.Intn(sum)
var k OpKind
for kind, w := range cfg.Weights {
if x < w { k = kind; break }
x -= w
}
switch k {
case OpPut:
vlen := cfg.ValLenMin + r.Intn(cfg.ValLenMax-cfg.ValLenMin+1)
return Op{Kind: OpPut, K: randKey(r, cfg.KeyLen), V: randBytes(r, vlen)}
case OpDel:
return Op{Kind: OpDel, K: randKey(r, cfg.KeyLen)}
case OpGet:
S := h.pickSnapshot(r)
return Op{Kind: OpGet, K: randKey(r, cfg.KeyLen), SnapSeq: S}
case OpRange:
    lo := randKey(r, cfg.KeyLen)
    hi := randKey(r, cfg.KeyLen)
    for bytes.Compare(hi, lo) <= 0 {
        hi = randKey(r, cfg.KeyLen)
    }
    S := h.pickSnapshot(r)
    return Op{Kind: OpRange, Lo: lo, Hi: hi, SnapSeq: S, Limit: 1000}
case OpSnap:
return Op{Kind: OpSnap}
default:
panic("unknown")
}
}

func (h *Harness) pickSnapshot(r *rand.Rand) uint64 {
if len(h.Snapshots) == 0 || r.Intn(10) == 0 {
// 10% use latest (“now”)
return h.Seq
}
return h.Snapshots[r.Intn(len(h.Snapshots))]
}

func (h *Harness) runForever(cfg Cfg) error {
r := rand.New(rand.NewSource(h.Seed))
// random initial snapshot for “now”
h.Snapshots = append(h.Snapshots, h.Seq)

enc := json.NewEncoder(h.LogFile)

for i := 0; ; i++ {
op := h.genOp(r, cfg)
if err := enc.Encode(struct{
I   int   `json:"i"`
Seq uint64`json:"seq"`
Op  Op    `json:"op"`
}{i, h.Seq, op}); err != nil {
return err
}

switch op.Kind {
case OpPut:
h.Seq++
if err := h.My.Put(op.K, op.V); err != nil { return h.fail(i, op, err) }
if err := h.Ref.PutWithSeq(op.K, op.V, h.Seq); err != nil { return h.fail(i, op, err) }
case OpDel:
h.Seq++
if err := h.My.Delete(op.K); err != nil { return h.fail(i, op, err) }
if err := h.Ref.DelWithSeq(op.K, h.Seq); err != nil { return h.fail(i, op, err) }
case OpGet:
    mv, mok, me := h.My.Get(op.K, op.SnapSeq)
    if me != nil { return h.fail(i, op, me) }
    sv, sok, se := h.Ref.GetWithSeq(op.K, op.SnapSeq)
    if se != nil { return h.fail(i, op, se) }
    if mok != sok || !equal(mv, sv) {
        return h.mismatch(i, op, mv, mok, sv, sok)
    }
case OpRange:
    mres, me := h.My.Range(op.Lo, op.Hi, op.SnapSeq, op.Limit)
    if me != nil { return h.fail(i, op, me) }
    sres, se := h.Ref.RangeWithSeq(op.Lo, op.Hi, op.SnapSeq, op.Limit)
    if se != nil { return h.fail(i, op, se) }
    if err := compareKVLists(mres, sres); err != nil {
        return h.fail(i, op, err)
    }
case OpSnap:
// Freeze current seq as a new snapshot
h.Snapshots = append(h.Snapshots, h.Seq)
}

// Periodic invariants
if i%10_000 == 0 {
if err := h.checkInvariants(); err != nil {
return h.fail(i, Op{Kind: 255}, err)
}
// optional: chaos hooks
// maybeCrash(i); maybeForceCompaction(); maybeGC();
}
}
}

// Helper stubs referenced above; fill in as needed.
func (h *Harness) fail(i int, op Op, cause error) error { /* flush log & wrap error */ }
func (h *Harness) mismatch(i int, op Op, mv []byte, mok bool, sv []byte, sok bool) error {
    /* convenience wrapper around fail for mismatched reads */
}
func compareKVLists(a, b []KV) error { /* compare lengths and each KV pair */ }
func (h *Harness) checkInvariants() error { /* periodic consistency checks */ }
func equal(a, b []byte) bool { /* byte-wise equality */ return bytes.Equal(a, b) }
```

---

## Main Entrypoint

```go
// diffharness/cmd/main.go
package main

import (
"crypto/rand"
"encoding/binary"
"flag"
"log"
"time"

"github.com/yourorg/rindb/diffharness"
)

func main() {
var seed int64
var logPath string
flag.Int64Var(&seed, "seed", 0, "PRNG seed (0=random)")
flag.StringVar(&logPath, "log", "repro.jsonl", "path to JSONL log")
flag.Parse()

if seed == 0 {
var b [8]byte
_, _ = rand.Read(b[:])
seed = int64(binary.LittleEndian.Uint64(b[:]))
}
log.Printf("seed=%d", seed)

my, err := diffharness.OpenRinDB("rindb-data")
if err != nil { log.Fatal(err) }
defer my.Close()

ref, err := diffharness.OpenSQLiteOracle("oracle.db")
if err != nil { log.Fatal(err) }
defer ref.Close()

h, err := diffharness.NewHarness(my, ref, seed, logPath)
if err != nil { log.Fatal(err) }
defer h.Close()

cfg := diffharness.Cfg{
KeyLen:    16,
ValLenMin: 0, ValLenMax: 1024,
RangeMax:  1000,
Weights: map[diffharness.OpKind]int{
diffharness.OpPut:   35,
diffharness.OpDel:   10,
diffharness.OpGet:   35,
diffharness.OpRange: 15,
diffharness.OpSnap:  5,
},
}

log.Printf("starting… %s", time.Now().Format(time.RFC3339))
if err := h.RunForever(cfg); err != nil { log.Fatal(err) }
}
```

Add the missing exported method:

```go
// in fuzzer.go
func (h *Harness) RunForever(cfg Cfg) error { return h.runForever(cfg) }
```

---

## Snapshot ✨ (Markdown Summary)

* **What is a snapshot?** A read-view identified by a **sequence number S**. A reader at S sees **the latest version with `seq ≤ S`** for each key.
* **How we create it:** `OpSnap` records the current global `seq` to the snapshot pool.
* **Reads at snapshot:**

  * **Get(k, S)** → pick version `max seq ≤ S`.
  * **Range([lo,hi), S)** → for each key in range, pick `max seq ≤ S`, exclude tombstones.
* **Why this matters:** Compactions rewrite storage, but **read visibility must not change**. The oracle encodes this rule; differences indicate logic bugs (e.g., tombstone leaks, snapshot invalidation, iterator stitching errors).

---

## JSONL Repro Log (Example)

Each line: the op as generated/executed.

```json
{"i":0,"seq":0,"op":{"Kind":0,"K":"base64…","V":"base64…","SnapSeq":0,"Limit":0}}
{"i":1,"seq":1,"op":{"Kind":4}}   // snapshot taken at seq=1
{"i":2,"seq":1,"op":{"Kind":3,"Lo":"…","Hi":"…","SnapSeq":1,"Limit":1000}}
{"i":3,"seq":1,"op":{"Kind":2,"K":"…","SnapSeq":1}}
```

When a mismatch occurs, the harness returns a descriptive error that includes `i`, `seq`, and `kind`, and the log contains the **full prefix** to replay.

---

## Metamorphic Testing

Re-run the same log (same `-seed` + captured config) with **varied engine configs**:

* block size (e.g., 4KiB, 16KiB, 64KiB)
* Bloom on/off / different false-positive rates
* compaction styles (leveled/universal/size-tiered, if supported)
* WAL sync modes (FULL/META/ASYNC)
* checksums on/off (Castagnoli vs IEEE)
* target file sizes, max open files, sharding/extents

**Expected:** identical user-visible results.

---

## Crash/Recovery Hooks (optional but powerful)

* Every N ops (e.g., 50k–200k), roll a die; on hit:

  1. **Hard crash**: `os.Exit(1)` after the op is **acknowledged** (or just before fsync for negative testing).
  2. Restart both DBs; your engine should **replay WAL**; the oracle is safe because we only append (`INSERT`).
  3. Resume fuzzing with the **same seed**, continuing `seq` from the harness counter.

> You can also add a **fault-injecting filesystem layer** around your I/O to simulate `EIO`, short writes, `ENOSPC`, etc.

---

## Periodic Invariants

* **Idempotent delete:** deleting an already deleted key at S doesn’t resurrect it.
* **Monotonic reads:** if a key is visible at `S1`, then at `S2 ≥ S1`, the value is **the same or newer** (never older).
* **Range consistency:** concatenating adjacent ranges equals a wider range.
* **No phantom resurrect:** after tombstone at `T ≤ S`, no older value appears in `Get/Range` at S.

---

## Telemetry (recommended)

* Log **ops/sec**, **latency percentiles** for Get/Range at different snapshot ages.
* Track **open FDs**, **goroutines**, **RSS** growth to spot leaks.
* Emit **compaction events** (level, input/output files, durations) to correlate with failures.

---

## Build & Run

**go.mod** (add what you need):

```go
module github.com/yourorg/rindb

go 1.22

require modernc.org/sqlite v1.32.0 // or latest
```

**Make targets (example):**

```make
diffharness:
go run ./diffharness/cmd -seed=0 -log=repro.jsonl

diffharness-seed:
@SEED=${SEED}; [ -z "$$SEED" ] && SEED=1; \
go run ./diffharness/cmd -seed=$$SEED -log=repro-$$SEED.jsonl
```

---

## Debugging Failures

1. Note the error: `fuzz fail at i=123456 seq=789012 kind=OpGet: ...`
2. Use `repro.jsonl` (or `-seed`) to **replay** up to `i` with **verbose tracing** enabled in your engine (memtable hits, table reads, bloom checks, iterator merges).
3. Dump **engine state summary**: manifest levels, file boundaries, tombstone counts per level, snapshot refs.
4. If needed, implement a small **delta-debugger** to binary-search the shortest prefix that still fails.

## Testing Notes

- The diff harness lives under `diffharness/` and is intended solely for differential testing. All harness files reside directly in the `diffharness` package with no subpackages.
- Coverage and unit-test runs that generate coverage data ignore `diffharness` to keep metrics focused on core packages.

