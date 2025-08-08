#!/usr/bin/env python3
import argparse, gzip, sqlite3, time, os

def pragmas(con):
    con.execute("PRAGMA journal_mode=OFF;")
    con.execute("PRAGMA synchronous=OFF;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA locking_mode=EXCLUSIVE;")
    con.execute("PRAGMA cache_size=-64000;")

def ensure_table(con, mode):
    if mode == "upsert":
        con.execute("""
            CREATE TABLE IF NOT EXISTS state(
                type TEXT, address TEXT, key TEXT, value TEXT,
                tx_hash TEXT, blob_index INTEGER, position INTEGER, timestamp INTEGER,
                PRIMARY KEY (address, key)
            )
        """)
    else:
        con.execute("""
            CREATE TABLE IF NOT EXISTS state(
                type TEXT, address TEXT, key TEXT, value TEXT,
                tx_hash TEXT, blob_index INTEGER, position INTEGER, timestamp INTEGER
            )
        """)

def iter_rows(gz, n, ts, batch=50_000):
    rows = []
    for _ in range(n):
        rtype = gz.read(1)[0]; gz.read(1)  # reserved
        address = gz.read(20).hex()
        k = gz.read(32).hex()
        v = gz.read(32).hex()
        th = gz.read(32).hex()
        bidx = int.from_bytes(gz.read(4), "big")
        p = int.from_bytes(gz.read(4), "big")
        rows.append(("state_diff" if rtype==2 else "tx", address, k, v, th, bidx, p, ts))
        if len(rows) >= batch:
            yield rows; rows = []
    if rows:
        yield rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blob", required=True)
    ap.add_argument("--db", default="lake/sqlite.db")
    ap.add_argument("--batch-size", type=int, default=50_000)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--mode", choices=["append","upsert"], default="upsert",
                    help="append: keep multi-versions; upsert: merge by PRIMARY KEY(address,key)")
    args = ap.parse_args()

    if args.reset and os.path.exists(args.db):
        os.remove(args.db)

    con = sqlite3.connect(args.db)
    pragmas(con)
    ensure_table(con, args.mode)
    cur = con.cursor()

    with gzip.open(args.blob, "rb") as gz:
        if gz.read(4) != b"ZKBL": raise SystemExit("bad magic")
        gz.read(2); gz.read(2)     # version, flags
        gz.read(4)                 # batch_id
        ts = int.from_bytes(gz.read(8), "big")
        n = int.from_bytes(gz.read(4), "big")
        gz.read(2)                 # rec_len

        t0=time.time()
        total=0
        for rows in iter_rows(gz, n, ts, args.batch_size):
            if args.mode == "upsert":
                cur.executemany("""
                  INSERT INTO state(type,address,key,value,tx_hash,blob_index,position,timestamp)
                  VALUES (?,?,?,?,?,?,?,?)
                  ON CONFLICT(address,key) DO UPDATE SET
                    value=excluded.value,
                    tx_hash=excluded.tx_hash,
                    blob_index=excluded.blob_index,
                    position=excluded.position,
                    timestamp=excluded.timestamp
                  WHERE (excluded.timestamp > state.timestamp)
                     OR (excluded.timestamp = state.timestamp AND excluded.position > state.position)
                """, rows)
            else:
                cur.executemany("INSERT INTO state VALUES (?,?,?,?,?,?,?,?)", rows)

            total += len(rows)
            if total % (args.batch_size*10) == 0:
                con.commit()
                print(f"[sqlite] processed {total:,} rows")
        con.commit()
        dt=time.time()-t0
        print(f"[sqlite:{args.mode}] DONE processed {total:,} rows in {dt:.1f}s")
    con.close()

if __name__ == "__main__":
    main()
