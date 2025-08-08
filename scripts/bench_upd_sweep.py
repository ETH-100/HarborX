#!/usr/bin/env python3
import argparse, json, os, subprocess, sys, tempfile, time

def run_py(script, *args):
    cmd = [sys.executable, script] + list(args)
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.stdout: print(p.stdout, end="")
    if p.stderr: print(p.stderr, end="")
    if p.returncode != 0: raise SystemExit(f"child failed: {' '.join(cmd)} (exit {p.returncode})")

def time_sqlite_query(db_path, sql):
    t0 = time.time()
    p = subprocess.run([sys.executable, os.path.join("scripts","query_arrow.py"),
                        "--sqlite", db_path, "--sql", sql],
                       capture_output=True, text=True)
    if p.stdout: print(p.stdout, end="")
    if p.stderr: print(p.stderr, end="")
    if p.returncode != 0: raise SystemExit("query sqlite failed")
    return time.time() - t0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ratios", default="0.1,0.25,0.5")
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--parts", type=int, default=4)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--chunk", type=int, default=200_000)
    ap.add_argument("--engine", choices=["auto","ext","bridge"], default="bridge")
    ap.add_argument("--sqlite-mode", choices=["append","upsert"], default="append")
    ap.add_argument("--out-csv", default="bench_upd_sweep.csv")
    args = ap.parse_args()

    ratios = [float(x.strip()) for x in args.ratios.split(",") if x.strip()]
    rows_out = []

    lww_sql = """
WITH ranked AS (
  SELECT key, value, timestamp, blob_index, position,
         ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC, blob_index DESC, position DESC) AS rn
  FROM state
)
SELECT COUNT(*) AS unique_keys FROM ranked WHERE rn=1;
"""

    for r in ratios:
        metrics_path = os.path.join(tempfile.gettempdir(), f"upd_metrics_{int(time.time()*1000)}.json")
        run_py(os.path.join("scripts","bench_updates_par.py"),
               "--rows", str(args.rows), "--parts", str(args.parts),
               "--update-ratio", str(r), "--workers", str(args.workers),
               "--chunk", str(args.chunk), "--engine", args.engine, "--out", metrics_path)

        with open(metrics_path, "r", encoding="utf-8") as f:
            m = json.load(f)

        sqlite_qs = m.get("sqlite_query_s")

        if args.sqlite_mode == "upsert":
            # Reload DB with upsert and re-time the same LWW query
            # 1) drop old DB
            if os.path.exists("lake/sqlite.db"):
                try: os.remove("lake/sqlite.db")
                except: pass
            # 2) load upsert
            run_py(os.path.join("scripts","blobs_to_sqlite_many.py"),
                   "--pattern","lake/blob_*.blob.gz","--db","lake/sqlite.db","--reset","--mode","upsert")
            # 3) re-time
            sqlite_qs = time_sqlite_query("lake/sqlite.db", lww_sql)

        rows_out.append([r, m.get("arrow_query_s"), sqlite_qs, m.get("total_rows"), m.get("keyspace")])

    with open(args.out_csv, "w", encoding="utf-8") as f:
        f.write("update_ratio,arrow_query_s,sqlite_query_s,total_rows,keyspace\n")
        for r in rows_out:
            f.write(",".join(str(x) for x in r) + "\n")
    print(f"[sweep] wrote {args.out_csv}")

if __name__ == "__main__":
    main()
