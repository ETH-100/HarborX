#!/usr/bin/env python3
import argparse, glob, os, subprocess, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pattern", default="lake/blob_*.blob.gz")
    ap.add_argument("--db", default="lake/sqlite.db")
    ap.add_argument("--batch-size", type=int, default=50_000)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--mode", choices=["append","upsert"], default="append")
    args = ap.parse_args()

    blobs = sorted(glob.glob(args.pattern))
    if not blobs:
        print(f"[sqlite-many] no blobs match {args.pattern}"); return

    first = True
    for b in blobs:
        cmd = [sys.executable, os.path.join("scripts","blob_to_sqlite.py"),
               "--blob", b, "--db", args.db, "--batch-size", str(args.batch_size), "--mode", args.mode]
        if args.reset and first: cmd.append("--reset")
        first = False
        print(">", " ".join(cmd)); rc = subprocess.call(cmd)
        if rc != 0: raise SystemExit(rc)

    print(f"[sqlite-many] done loading {len(blobs)} blob(s) into {args.db}")

if __name__ == "__main__":
    main()
