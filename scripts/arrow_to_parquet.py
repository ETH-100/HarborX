#!/usr/bin/env python3
"""
Optional helper: convert Arrow IPC files to Parquet via DuckDB.
Usage:
  python scripts/arrow_to_parquet.py --arrowdir lake/hot --out lake/parquet/state.parquet
"""
import argparse, duckdb, os
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arrowdir", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    paths = sorted([str(p) for p in Path(args.arrowdir).glob("*.arrow")])
    if not paths: raise SystemExit(f"No .arrow in {args.arrowdir}")
    con = duckdb.connect()
    try:
        con.execute("INSTALL 'arrow'; LOAD 'arrow';")
        union = " UNION ALL ".join([f"SELECT * FROM read_ipc('{p.replace('\\','/')}')" for p in paths])
        con.execute(f"CREATE OR REPLACE VIEW state AS {union}")
    except Exception:
        import pyarrow.ipc as ipc
        views=[]
        for i,p in enumerate(paths):
            t = ipc.open_file(p).read_all()
            name=f"t{i}"; con.register(name,t); views.append(f"SELECT * FROM {name}")
        con.execute("CREATE OR REPLACE VIEW state AS " + " UNION ALL ".join(views))
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    con.execute(f"COPY (SELECT * FROM state) TO '{args.out.replace('\\','/')}' (FORMAT PARQUET)")
    print(f"[convert] wrote {args.out}")

if __name__ == "__main__":
    main()
