#!/usr/bin/env python3
# report_windows.py -- one-click window-size benchmark and Markdown report
import argparse, os, subprocess, sys, time, glob
from pathlib import Path

def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.stdout: print(p.stdout, end="")
    if p.stderr: print(p.stderr, end="")
    if p.returncode != 0:
        raise SystemExit(f"child failed: {' '.join(cmd)} (exit {p.returncode})")
    return p

def run_duckdb_live(arrow_snapshot:str, recent_arrows:list)->float:
    import duckdb, pyarrow.ipc as ipc, time
    con = duckdb.connect()
    sc = ipc.open_file(arrow_snapshot).read_all()
    con.register("state_current", sc)
    views=[]; 
    for i,p in enumerate(recent_arrows):
        tbl = ipc.open_file(p).read_all()
        name=f"r{i}"; con.register(name, tbl); views.append(f"SELECT * FROM {name}")
    if views:
        con.execute("CREATE OR REPLACE VIEW recent_deltas AS " + " UNION ALL ".join(views))
    else:
        con.execute("CREATE OR REPLACE VIEW recent_deltas AS SELECT * FROM state_current WHERE 0=1")
    sql = """
WITH d_last AS (
  SELECT key,value,timestamp,blob_index,position,type,address,tx_hash FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY key
             ORDER BY timestamp DESC, blob_index DESC, position DESC
           ) rn
    FROM recent_deltas
  ) WHERE rn=1
),
d_only AS (
  SELECT d.*
  FROM d_last d
  LEFT JOIN state_current sc ON sc.key = d.key
  WHERE sc.key IS NULL
     OR (d.timestamp, d.blob_index, d.position) >
        (sc.timestamp, sc.blob_index, sc.position)
)
SELECT COUNT(*) AS keys FROM (
  SELECT key FROM state_current
  UNION ALL
  SELECT key FROM d_only
);
"""
    t0=time.time(); out = con.execute(sql).fetchall(); dt=time.time()-t0
    print(out)
    print(f"[arrow-live] {len(out)} rows in {dt:.3f}s")
    return dt

def run_sqlite_live(db:str, target_table:str, max_index:int)->float:
    import sqlite3, time
    con = sqlite3.connect(db); cur = con.cursor()
    cur.execute("DROP VIEW IF EXISTS recent_deltas")
    cur.execute(f"CREATE TEMP VIEW recent_deltas AS SELECT * FROM state WHERE blob_index > {max_index}")
    cur.execute("DROP VIEW IF EXISTS state_current")
    cur.execute(f"CREATE TEMP VIEW state_current AS SELECT * FROM {target_table}")
    sql = """
WITH d_last AS (
  SELECT key,value,timestamp,blob_index,position,type,address,tx_hash FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY key
             ORDER BY timestamp DESC, blob_index DESC, position DESC
           ) rn
    FROM recent_deltas
  ) WHERE rn=1
),
d_only AS (
  SELECT d.*
  FROM d_last d
  LEFT JOIN state_current sc ON sc.key = d.key
  WHERE sc.key IS NULL
     OR (d.timestamp, d.blob_index, d.position) >
        (sc.timestamp, sc.blob_index, sc.position)
)
SELECT COUNT(*) AS keys FROM (
  SELECT key FROM state_current
  UNION ALL
  SELECT key FROM d_only
);
"""
    t0=time.time(); cur.execute(sql); rows=cur.fetchall(); dt=time.time()-t0
    print(rows)
    print(f"[sqlite-live] {len(rows)} rows in {dt:.3f}s")
    con.close(); return dt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500000)
    ap.add_argument("--parts", type=int, default=16)
    ap.add_argument("--update-ratio", type=float, default=0.5)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--chunk", type=int, default=200000)
    ap.add_argument("--windows", default="1,4,16,64")
    ap.add_argument("--engine", choices=["auto","ext","bridge"], default="bridge")
    ap.add_argument("--out", default="report_windows.md")
    ap.add_argument("--no-exact", action="store_true", help="do not use --exact-ratio in gen")
    args = ap.parse_args()
    print(f"[python] using: {sys.executable}")

    total = args.rows * args.parts
    K = max(1, int(total * (1.0 - args.update_ratio)))
    windows = [int(x.strip()) for x in args.windows.split(",") if x.strip()]
    print(f"=== SETTINGS ===\\nrows/part={args.rows:,} parts={args.parts} total={total:,} update_ratio≈{args.update_ratio} keyspace≈{K:,} windows={windows}\\n")

    # Clean
    for p in glob.glob("lake/blob_*.blob.gz"):
        try: os.remove(p)
        except: pass
    os.makedirs("lake/hot", exist_ok=True)
    for f in Path("lake/hot").glob("*.arrow"):
        try: f.unlink()
        except: pass
    if Path("lake/sqlite.db").exists(): os.remove("lake/sqlite.db")
    os.makedirs("lake/base", exist_ok=True)

    # 1) gen
    gen_cmd = [sys.executable, os.path.join("scripts","gen_blob.py"),
               "--rows", str(args.rows), "--parts", str(args.parts),
               "--out", "lake/blob", "--keyspace", str(K)]
    if not args.no_exact:
        gen_cmd.append("--exact-ratio")
    run(gen_cmd)

    # 2) blobs -> arrow
    t0=time.time()
    run([sys.executable, os.path.join("scripts","blob_to_arrow_many.py"),
         "--pattern","lake/blob_*.blob.gz","--outdir","lake/hot","--workers",str(args.workers),
         "--chunk",str(args.chunk),"--skip-exists"])
    dt_arrow_write = time.time()-t0

    # 3) load SQLite (append baseline)
    t0=time.time()
    run([sys.executable, os.path.join("scripts","blobs_to_sqlite_many.py"),
         "--pattern","lake/blob_*.blob.gz","--db","lake/sqlite.db","--reset","--mode","append"])
    dt_sqlite_append = time.time()-t0

    # Markdown
    lines = []
    lines.append(f"# HarborX Window-Size Benchmark Report\\n")
    lines.append(f"- Total rows: **{total:,}** ({args.parts}×{args.rows:,})  \\\\ Update ratio: **{args.update_ratio:.2f}** → keyspace≈**{K:,}**")
    lines.append(f"- Workers: **{args.workers}**, chunk: **{args.chunk:,}**\\n")
    lines.append(f"- Arrow write (all parts): **{dt_arrow_write:.3f}s**, SQLite append (all parts): **{dt_sqlite_append:.3f}s**\\n")

    N = args.parts
    # 4) For each window W
    for W in windows:
        base_max = N - W
        if base_max < 0: 
            continue
        # Arrow compact base
        out_snap = f"lake/base/state_current_W{W:02d}.arrow"
        t0=time.time()
        run([sys.executable, os.path.join("scripts","compact_arrow.py"),
             "--arrowdir","lake/hot","--out", out_snap, "--max-index", str(base_max)])
        dt_arrow_compact = time.time()-t0
        # Arrow live query
        recent = [str(p).replace("\\","/") for p in sorted(Path("lake/hot").glob("*.arrow")) if int(p.stem.split("_")[-1]) > base_max]
        dt_arrow_live = run_duckdb_live(out_snap, recent)

        # SQLite compact base
        t0=time.time()
        run([sys.executable, os.path.join("scripts","compact_sqlite.py"),
             "--db","lake/sqlite.db","--source","state","--target",f"state_current_W{W:02d}",
             "--max-blob-index", str(base_max)])
        dt_sqlite_compact = time.time()-t0
        # SQLite live query
        dt_sqlite_live = run_sqlite_live("lake/sqlite.db", f"state_current_W{W:02d}", base_max)

        TTF_arrow = dt_arrow_compact + dt_arrow_live
        TTF_sqlite = dt_sqlite_compact + dt_sqlite_live

        lines.append(f"## Window W = {W}\\n")
        lines.append("| Path | Compact (base) | Live query | TTF (base+live) |")
        lines.append("|---|---:|---:|---:|")
        lines.append(f"| Arrow | {dt_arrow_compact:.3f}s | {dt_arrow_live:.3f}s | **{TTF_arrow:.3f}s** |")
        lines.append(f"| SQLite | {dt_sqlite_compact:.3f}s | {dt_sqlite_live:.3f}s | **{TTF_sqlite:.3f}s** |\\n")

    Path(args.out).write_text("\\n".join(lines), encoding="utf-8")
    print(f"[report] wrote {args.out}")

if __name__ == "__main__":
    main()
