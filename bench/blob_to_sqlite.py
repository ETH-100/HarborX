#!/usr/bin/env python3
import argparse, gzip, sqlite3, time, os

def pragmas(con):
    con.execute("PRAGMA journal_mode=OFF;")
    con.execute("PRAGMA synchronous=OFF;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA locking_mode=EXCLUSIVE;")
    con.execute("PRAGMA cache_size=-64000;")

def ensure_table(con, mode:str):
    con.execute("""CREATE TABLE IF NOT EXISTS state(
        type TEXT,
        address BLOB,
        key BLOB,
        value BLOB,
        tx_hash BLOB,
        blob_index INTEGER,
        position INTEGER,
        timestamp INTEGER
    )""")
    if mode == "upsert":
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_state_key ON state(key)")

def iter_rows(gz, n, ts, batch=50_000):
    rows = []
    for _ in range(n):
        rtype = gz.read(1)[0]; gz.read(1)
        address = gz.read(20)
        k = gz.read(32)
        v = gz.read(32)
        th = gz.read(32)
        bidx = int.from_bytes(gz.read(4), "big")
        p = int.from_bytes(gz.read(4), "big")
        rows.append(("state_diff" if rtype==2 else "tx", address, k, v, th, bidx, p, ts))
        if len(rows) >= batch:
            yield rows; rows = []
    if rows: yield rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blob", required=True)
    ap.add_argument("--db", default="lake/sqlite.db")
    ap.add_argument("--batch-size", type=int, default=50_000)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--mode", choices=["append","upsert"], default="append")
    args = ap.parse_args()

    if not args.blob.endswith(".blob.gz"):
        raise SystemExit("blob_to_sqlite expects a .blob.gz input")
    if args.reset and os.path.exists(args.db):
        os.remove(args.db)

    con = sqlite3.connect(args.db); pragmas(con); ensure_table(con, args.mode); cur = con.cursor()
    with gzip.open(args.blob, "rb") as gz:
        if gz.read(4) != b"ZKBL": raise SystemExit("bad magic")
        gz.read(2); gz.read(2); gz.read(4)
        ts = int.from_bytes(gz.read(8), "big")
        n = int.from_bytes(gz.read(4), "big")
        gz.read(2)

        t0=time.time(); total=0
        if args.mode == "append":
            sql = "INSERT INTO state VALUES (?,?,?,?,?,?,?,?)"
        else:
            sql = """INSERT INTO state(type,address,key,value,tx_hash,blob_index,position,timestamp)
                     VALUES (?,?,?,?,?,?,?,?)
                     ON CONFLICT(key) DO UPDATE SET
                        type=excluded.type,
                        address=excluded.address,
                        value=excluded.value,
                        tx_hash=excluded.tx_hash,
                        blob_index=excluded.blob_index,
                        position=excluded.position,
                        timestamp=excluded.timestamp
                     WHERE excluded.timestamp > state.timestamp OR
                           (excluded.timestamp = state.timestamp AND
                             (excluded.blob_index > state.blob_index OR
                              (excluded.blob_index = state.blob_index AND excluded.position > state.position)));"""
        for rows in iter_rows(gz, n, ts, args.batch_size):
            cur.executemany(sql, rows)
            total += len(rows)
            if total % (args.batch_size*10) == 0:
                con.commit(); print(f"[sqlite] inserted {total:,} rows")
        con.commit(); dt=time.time()-t0
        print(f"[sqlite] DONE {('upserted' if args.mode=='upsert' else 'inserted')} {total:,} rows in {dt:.1f}s")
    con.close()

if __name__ == "__main__":
    main()
