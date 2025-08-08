#!/usr/bin/env python3
import argparse, glob, os, subprocess, sys, time
from pathlib import Path

def run_py(script, *args):
    cmd = [sys.executable, script] + list(args)
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.stdout: print(p.stdout, end="")
    if p.stderr: print(p.stderr, end="")
    if p.returncode != 0: raise SystemExit(f"child failed: {' '.join(cmd)} (exit {p.returncode})")
    return p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--parts", type=int, default=4)
    ap.add_argument("--update-ratio", type=float, default=0.5)
    ap.add_argument("--hot-frac", type=float, default=0.05)
    ap.add_argument("--hot-amp", type=float, default=20.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--chunk", type=int, default=200_000)
    ap.add_argument("--arrowdir", default="lake/hot")
    ap.add_argument("--db", default="lake/sqlite.db")
    ap.add_argument("--engine", choices=["auto","ext","bridge"], default="bridge")
    ap.add_argument("--out", help="write metrics JSON")
    args = ap.parse_args()

    total_rows = args.rows * args.parts
    K = max(1, int(total_rows * (1.0 - args.update_ratio)))
    print(f"=== SETTINGS ===\nparts={args.parts} rows/part={args.rows:,} total={total_rows:,} update_ratio≈{args.update_ratio:.2f} keyspace≈{K:,}\n")

    # Clean artifacts
    for p in glob.glob("lake/blob_*.blob.gz"): 
        try: os.remove(p)
        except: pass
    Path(args.arrowdir).mkdir(parents=True, exist_ok=True)
    for p in Path(args.arrowdir).glob("*.arrow"):
        try: p.unlink()
        except: pass
    if os.path.exists(args.db): os.remove(args.db)

    print("=== 1) gen blobs (with updates) ===")
    run_py(os.path.join("scripts","gen_blob.py"),
           "--rows", str(args.rows), "--parts", str(args.parts),
           "--out", "lake/blob", "--keyspace", str(K),
           "--hot-frac", str(args.hot_frac), "--hot-amp", str(args.hot_amp))

    print("=== 2) parallel blob -> arrow ===")
    run_py(os.path.join("scripts","blob_to_arrow_many.py"),
           "--pattern", "lake/blob_*.blob.gz",
           "--outdir", args.arrowdir, "--workers", str(args.workers),
           "--chunk", str(args.chunk), "--skip-exists")

    print("=== 3) LWW query on Arrow ===")
    lww_sql = """
WITH ranked AS (
  SELECT key, value, timestamp, blob_index, position,
         ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC, blob_index DESC, position DESC) AS rn
  FROM state
)
SELECT COUNT(*) AS unique_keys FROM ranked WHERE rn=1;
"""
    t0=time.time()
    run_py(os.path.join("scripts","query_arrow.py"), "--arrow", args.arrowdir, "--engine", args.engine, "--sql", lww_sql)
    dt_arrow = time.time()-t0
    print(f"[upd-bench] Arrow LWW query: {dt_arrow:.3f}s")

    print("=== 4) load all blobs into SQLite ===")
    run_py(os.path.join("scripts","blobs_to_sqlite_many.py"),
           "--pattern", "lake/blob_*.blob.gz", "--db", args.db, "--reset")

    print("=== 5) LWW query on SQLite ===")
    t0=time.time()
    run_py(os.path.join("scripts","query_arrow.py"),
           "--sqlite", args.db, "--sql", lww_sql)
    dt_sqlite = time.time()-t0
    print(f"[upd-bench] SQLite LWW query: {dt_sqlite:.3f}s")

    if args.out:
        import json as _json
        with open(args.out, "w", encoding="utf-8") as f:
            _json.dump({
                "update_ratio": args.update_ratio,
                "total_rows": total_rows,
                "keyspace": K,
                "arrow_query_s": dt_arrow,
                "sqlite_query_s": dt_sqlite
            }, f)
        print(f"[upd-bench] metrics -> {args.out}")

    print("=== SUMMARY ===")
    print(f"Update ratio ~ {args.update_ratio:.2f}, keyspace ≈ {K:,}, total rows={total_rows:,}")
    print(f"Arrow LWW:   {dt_arrow:.3f}s")
    print(f"SQLite LWW:  {dt_sqlite:.3f}s")
    if dt_sqlite > 0:
        print(f"Speedup (Arrow over SQLite): {dt_sqlite/dt_arrow:.2f}×")

if __name__ == "__main__":
    main()
