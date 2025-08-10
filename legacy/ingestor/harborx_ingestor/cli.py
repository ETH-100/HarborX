from __future__ import annotations
import argparse, os, sys, subprocess
from pathlib import Path
from .ingest import ingest_folder

def make_demo(out:str, rows:int, parts:int, seed:int):
    repo  = Path(__file__).resolve().parents[4]  # repo root
    gen = repo / "bench" / "gen_blob.py"
    os.makedirs(out, exist_ok=True)
    cmd = [sys.executable, str(gen), "--out", os.path.join(out, "blob"),
           "--rows", str(rows), "--parts", str(parts), "--seed", str(seed)]
    print("[make-demo]", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    ap = argparse.ArgumentParser(prog="harborx")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s0 = sub.add_parser("make-demo", help="Generate demo .blob.gz files")
    s0.add_argument("--out", required=True)
    s0.add_argument("--rows", type=int, default=20000)
    s0.add_argument("--parts", type=int, default=1)
    s0.add_argument("--seed", type=int, default=42)

    s1 = sub.add_parser("ingest", help="Ingest .blob.gz folder → Parquet dataset")
    s1.add_argument("--source", required=True)
    s1.add_argument("--chain", type=int, required=True)
    s1.add_argument("--out", required=True)
    s1.add_argument("--row-group", type=int, default=8192)

    args = ap.parse_args()
    if args.cmd == "make-demo":
        make_demo(args.out, args.rows, args.parts, args.seed)
    elif args.cmd == "ingest":
        ingest_folder(args.source, args.out, args.chain, max_row_group=args.row_group)

if __name__ == "__main__":
    main()
