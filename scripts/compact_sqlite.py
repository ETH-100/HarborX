#!/usr/bin/env python3
# compact_sqlite.py -- LWW-compact a subset of rows into a snapshot table
import argparse, sqlite3, time, os

SQL_LWW = """
CREATE TABLE {target} AS
WITH ranked AS (
  SELECT type,address,key,value,tx_hash,blob_index,position,timestamp,
         ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC, blob_index DESC, position DESC) rn
  FROM {source}
  {where}
)
SELECT type,address,key,value,tx_hash,blob_index,position,timestamp
FROM ranked WHERE rn=1;
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="lake/sqlite.db")
    ap.add_argument("--source", default="state")
    ap.add_argument("--target", default="state_current")
    ap.add_argument("--min-timestamp", type=int, help="only compact rows with timestamp >= this (optional)")
    ap.add_argument("--max-timestamp", type=int, help="only compact rows with timestamp <= this (optional)")
    ap.add_argument("--min-blob-index", type=int, help="only rows with blob_index >= this (optional)")
    ap.add_argument("--max-blob-index", type=int, help="only rows with blob_index <= this (optional)")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    con = sqlite3.connect(args.db); cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS " + args.target)
    where = []
    if args.min_timestamp is not None: where.append(f"timestamp >= {int(args.min_timestamp)}")
    if args.max_timestamp is not None: where.append(f"timestamp <= {int(args.max_timestamp)}")
    if args.min_blob_index is not None: where.append(f"blob_index >= {int(args.min_blob_index)}")
    if args.max_blob_index is not None: where.append(f"blob_index <= {int(args.max_blob_index)}")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    t0=time.time()
    cur.executescript(SQL_LWW.format(source=args.source, target=args.target, where=where_sql))
    con.commit()
    dt=time.time()-t0
    cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{args.target}_key ON {args.target}(key)")
    con.commit()
    n = cur.execute(f"SELECT COUNT(*) FROM {args.target}").fetchone()[0]
    print(f"[compact-sqlite] {args.source} -> {args.target}: {n} rows in {dt:.3f}s")
    con.close()

if __name__ == "__main__":
    main()
