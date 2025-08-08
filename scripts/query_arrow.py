#!/usr/bin/env python3
import argparse, os, sqlite3, sys, time
from pathlib import Path
def run_datafusion(sql:str, arrow_dir:str):
    from datafusion import SessionContext
    ctx = SessionContext()
    try:
        ctx.register_parquet("state", arrow_dir, listing_options={"format":"arrow"})
    except Exception as e:
        print("[datafusion] Arrow directory listing not supported in this build.\n", e, file=sys.stderr)
        raise
    df = ctx.sql(sql)
    t0=time.time(); res = df.collect(); dt=time.time()-t0
    print(f"[datafusion] {len(res)} batch(es) in {dt:.3f}s")
    for b in res: print(b.to_pandas().head(20))
    return dt
def run_duckdb(sql:str, arrow_dir:str):
    import duckdb, pyarrow.ipc as ipc
    con = duckdb.connect(); con.execute("PRAGMA threads=4;")
    paths = sorted([str(p) for p in Path(arrow_dir).glob("*.arrow")])
    if not paths: raise SystemExit(f"No .arrow in {arrow_dir}")
    norms = [p.replace("\\", "/") for p in paths]
    try:
        con.execute("INSTALL 'arrow'; LOAD 'arrow';")
        union_parts = ["SELECT * FROM read_ipc('{}')".format(p) for p in norms]
        con.execute("CREATE OR REPLACE VIEW state AS " + " UNION ALL ".join(union_parts))
        print("[duckdb] using arrow extension read_ipc()")
    except Exception:
        print("[duckdb] arrow extension unavailable; falling back to PyArrow bridge (loads batches into memory).", file=sys.stderr)
        views=[]
        for i,p in enumerate(norms):
            tbl = ipc.open_file(p).read_all()
            name = f"t{i}"; con.register(name, tbl); views.append(f"SELECT * FROM {name}")
        con.execute("CREATE OR REPLACE VIEW state AS " + " UNION ALL ".join(views))
    t0=time.time()
    table = con.execute(sql).fetch_arrow_table()
    dt=time.time()-t0
    print(table.slice(0, min(20, table.num_rows)).to_pydict())
    print(f"[duckdb] {table.num_rows} rows in {dt:.3f}s")
    return dt
def run_sqlite(sql:str, db_path:str):
    t0=time.time(); con = sqlite3.connect(db_path); cur = con.cursor()
    cur.execute(sql); rows = cur.fetchall(); dt=time.time()-t0
    print(rows[:10] if rows else "[]"); print(f"[sqlite] {len(rows)} rows in {dt:.3f}s")
    con.close(); return dt
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql", required=True)
    ap.add_argument("--arrow", default="./lake/hot")
    ap.add_argument("--sqlite", help="SQLite db path")
    ap.add_argument("--duckdb", action="store_true")
    args = ap.parse_args()
    if args.sqlite: run_sqlite(args.sql, args.sqlite)
    else:
        if args.duckdb:
            run_duckdb(args.sql, args.arrow)
        else:
            try: run_datafusion(args.sql, args.arrow)
            except Exception:
                print("[warn] Falling back to DuckDB for Arrow querying...")
                run_duckdb(args.sql, args.arrow)
if __name__ == "__main__": main()
