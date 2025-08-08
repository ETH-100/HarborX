#!/usr/bin/env python3
import argparse, os, sqlite3, sys, time
from pathlib import Path

def run_duckdb_ext(sql:str, arrow_dir:str):
    import duckdb
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={os.cpu_count() or 4};")
    paths = sorted([str(p).replace("\\","/") for p in Path(arrow_dir).glob("*.arrow")])
    if not paths: raise SystemExit(f"No .arrow in {arrow_dir}")
    con.execute("INSTALL 'arrow'; LOAD 'arrow';")
    union = " UNION ALL ".join([f"SELECT * FROM read_ipc('{p}')" for p in paths])
    con.execute("CREATE OR REPLACE VIEW state AS " + union)
    t0=time.time(); table = con.execute(sql).fetch_arrow_table(); dt=time.time()-t0
    print(table.slice(0, min(20, table.num_rows)).to_pydict())
    print(f"[duckdb-ext] {table.num_rows} rows in {dt:.3f}s")
    return dt

def run_duckdb_bridge(sql:str, arrow_dir:str):
    import duckdb, pyarrow.ipc as ipc
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={os.cpu_count() or 4};")
    paths = sorted([str(p).replace("\\","/") for p in Path(arrow_dir).glob("*.arrow")])
    if not paths: raise SystemExit(f"No .arrow in {arrow_dir}")
    views = []
    for i,p in enumerate(paths):
        tbl = ipc.open_file(p).read_all()
        name = f"t{i}"
        con.register(name, tbl)
        views.append(f"SELECT * FROM {name}")
    con.execute("CREATE OR REPLACE VIEW state AS " + " UNION ALL ".join(views))
    t0=time.time(); table = con.execute(sql).fetch_arrow_table(); dt=time.time()-t0
    print(table.slice(0, min(20, table.num_rows)).to_pydict())
    print(f"[duckdb-bridge] {table.num_rows} rows in {dt:.3f}s")
    return dt

def run_sqlite(sql:str, db_path:str):
    t0=time.time(); import sqlite3 as _s
    con = _s.connect(db_path); cur = con.cursor()
    cur.execute(sql); rows = cur.fetchall(); dt=time.time()-t0
    print(rows[:10] if rows else "[]"); print(f"[sqlite] {len(rows)} rows in {dt:.3f}s")
    con.close(); return dt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql", required=True)
    ap.add_argument("--arrow", default="./lake/hot")
    ap.add_argument("--sqlite", help="SQLite db path")
    ap.add_argument("--engine", choices=["auto","ext","bridge"], default="bridge")
    args = ap.parse_args()

    if args.sqlite: run_sqlite(args.sql, args.sqlite)
    else:
        if args.engine == "ext": run_duckdb_ext(args.sql, args.arrow)
        elif args.engine == "bridge": run_duckdb_bridge(args.sql, args.arrow)
        else:
            try: run_duckdb_ext(args.sql, args.arrow)
            except Exception as e:
                print("[warn] ext engine failed, falling back to bridge:", e)
                run_duckdb_bridge(args.sql, args.arrow)

if __name__ == "__main__":
    main()
