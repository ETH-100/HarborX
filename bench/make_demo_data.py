#!/usr/bin/env python3
import os, sys, subprocess, argparse, pathlib
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rows', type=int, default=20000)
    ap.add_argument('--parts', type=int, default=1)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--chain', type=int, default=167001)
    ap.add_argument('--out', default='./data')
    args = ap.parse_args()
    repo = pathlib.Path(__file__).resolve().parents[1]
    blobs = pathlib.Path(args.out)/'blobs'; parquet = pathlib.Path(args.out)/'parquet'
    blobs.mkdir(parents=True, exist_ok=True); parquet.mkdir(parents=True, exist_ok=True)
    subprocess.check_call([sys.executable, str(repo/'scripts'/'gen_blob.py'),
                           '--out', str(blobs/'blob'), '--rows', str(args.rows),
                           '--parts', str(args.parts), '--seed', str(args.seed)])
    subprocess.check_call([sys.executable, '-m', 'harborx_ingestor.cli', 'ingest',
                           '--source', str(blobs), '--chain', str(args.chain), '--out', str(parquet)])
if __name__ == '__main__': main()
