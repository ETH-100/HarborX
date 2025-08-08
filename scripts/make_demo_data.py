#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, shutil, sys, json
from pathlib import Path
import subprocess as sp

def run_py(script, *args):
    cmd = [sys.executable, script] + list(args)
    print(">", " ".join(cmd))
    p = sp.run(cmd, capture_output=True, text=True)
    if p.stdout: print(p.stdout, end="")
    if p.stderr: print(p.stderr, end="")
    if p.returncode != 0:
        raise SystemExit(f"child failed: {' '.join(cmd)} (exit {p.returncode})")

def human(n: int) -> str:
    x = float(n)
    for unit in ["B","KB","MB","GB","TB"]:
        if x < 1024.0: return f"{x:.1f}{unit}"
        x /= 1024.0
    return f"{x:.1f}TB"

def dir_size(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())

def to_manifest_path(p: Path) -> str:
    """Make paths relative to web/ when present (so manifest uses 'demo/…')."""
    s = str(p).replace("\\", "/")
    return s[len("web/"):] if s.startswith("web/") else s

def main():
    ap = argparse.ArgumentParser(description="Make tiny demo dataset under ./web/demo/ (Parquet-first)")
    ap.add_argument("--rows", type=int, default=8000, help="rows per part (small for GitHub Pages)")
    ap.add_argument("--parts", type=int, default=4, help="number of blob files")
    ap.add_argument("--update-ratio", type=float, default=0.5)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--chunk", type=int, default=4000)
    ap.add_argument("--out-dir", default="web/demo", help="output root (writes under <out-dir>/lake and <out-dir>/parquet)")
    ap.add_argument("--drop-arrow", action="store_true", help="delete Arrow files after Parquet conversion")
    args = ap.parse_args()

    out_root = Path(args.out_dir)
    lake = out_root / "lake"
    hot = lake / "hot"                # Arrow IPC files here
    blob_prefix = lake / "blob"
    pq_dir = out_root / "parquet"     # Parquet files here

    # Clean previous demo
    if out_root.exists():
        shutil.rmtree(out_root)
    hot.mkdir(parents=True, exist_ok=True)
    pq_dir.mkdir(parents=True, exist_ok=True)

    total = args.rows * args.parts
    keyspace = max(1, int(total * (1.0 - args.update_ratio)))
    print(f"[demo] rows/part={args.rows:,} parts={args.parts} total={total:,} update_ratio~{args.update_ratio} keyspace~{keyspace:,}")

    # 1) gen tiny blobs
    run_py(os.path.join("scripts","gen_blob.py"),
           "--rows", str(args.rows), "--parts", str(args.parts),
           "--out", str(blob_prefix).replace("\\","/"),
           "--keyspace", str(keyspace))

    # 2) blobs -> arrow (tiny)
    run_py(os.path.join("scripts","blob_to_arrow_many.py"),
           "--pattern", str(lake / "blob_*.blob.gz"),
           "--outdir", str(hot),
           "--workers", str(args.workers),
           "--chunk", str(args.chunk),
           "--skip-exists")

    # 3) arrow -> parquet
    try:
        import pyarrow.ipc as _ipc
        import pyarrow.parquet as _pq
    except Exception as e:
        raise SystemExit(f"[demo] pyarrow is required: pip install pyarrow  ({e})")

    parquet_paths = []
    arrow_paths = sorted(hot.glob("*.arrow"))
    if not arrow_paths:
        raise SystemExit(f"[demo] no Arrow files found in {hot}")

    for apath in arrow_paths:
        tbl = _ipc.open_file(apath).read_all()
        outp = pq_dir / (apath.stem + ".parquet")
        _pq.write_table(tbl, outp)
        parquet_paths.append(outp)

    # 4) optional: drop arrow to keep repo small
    if args.drop_arrow:
        shutil.rmtree(hot, ignore_errors=True)

    # Size report
    sizes = {
        "parquet_dir": human(dir_size(pq_dir)) if pq_dir.exists() else "0B",
        "arrow_dir": human(dir_size(hot)) if hot.exists() else "0B",
    }
    print(f"[demo] sizes: {json.dumps(sizes)}")
    print(f"[demo] done. Parquet -> {pq_dir}" + (f" ; Arrow -> {hot}" if hot.exists() else ""))

    # 5) write manifest.json (prefer parquet; keep arrow list for fallback if retained)
    manifest = {
        "parquet": [to_manifest_path(p) for p in sorted(parquet_paths)],
        "arrow": [to_manifest_path(p) for p in sorted(hot.glob('*.arrow'))] if hot.exists() else []
    }
    (out_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[demo] wrote {out_root/'manifest.json'} with {len(manifest['parquet'])} parquet file(s)")

if __name__ == "__main__":
    main()
