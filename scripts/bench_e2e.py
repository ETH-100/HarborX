#!/usr/bin/env python3
import argparse, subprocess, time, os, sys
def run_py(script, *args):
    cmd = [sys.executable, script] + list(args)
    t0=time.time()
    p = subprocess.run(cmd, check=True, capture_output=True, text=True)
    print(p.stdout, end=""); 
    if p.stderr: print(p.stderr, end="")
    return time.time()-t0
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--sql", default='SELECT COUNT(*) FROM state WHERE position < 100000')
    args = ap.parse_args()
    blob = os.path.join("lake", "blob_000001.blob.gz")
    arrow = os.path.join("lake", "hot", "blob_000001.arrow")
    sqlite_db = os.path.join("lake", "sqlite.db")
    print("=== 1) gen blob ===")
    dt = run_py(os.path.join("scripts","gen_blob.py"), "--rows", str(args.rows), "--parts", "1", "--out", "lake/blob")
    print(f"[bench] gen_blob: {dt:.3f}s")
    print("=== 2) blob -> arrow ===")
    dt = run_py(os.path.join("scripts","blob_to_arrow.py"), "--blob", blob, "--out", arrow)
    print(f"[bench] blob->arrow: {dt:.3f}s")
    print("=== 3) query arrow ===")
    dt = run_py(os.path.join("scripts","query_arrow.py"), "--arrow", os.path.join("lake","hot"), "--duckdb", "--sql", args.sql)
    print(f"[bench] query arrow: {dt:.3f}s")
    print("=== 4) blob -> sqlite ===")
    if os.path.exists(sqlite_db): os.remove(sqlite_db)
    dt = run_py(os.path.join("scripts","blob_to_sqlite.py"), "--blob", blob, "--db", sqlite_db, "--reset")
    print(f"[bench] blob->sqlite: {dt:.3f}s")
    print("=== 5) query sqlite ===")
    dt = run_py(os.path.join("scripts","query_arrow.py"), "--sqlite", sqlite_db, "--sql", args.sql)
    print(f"[bench] query sqlite: {dt:.3f}s")
if __name__ == "__main__": main()
