#!/usr/bin/env python3
import argparse, os, json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--data", default="data")
    ap.add_argument("--include-parquet", action="store_true")
    args = ap.parse_args()
    root = os.path.abspath(args.root)
    ddir = os.path.join(root, args.data)
    arrow, parquet = [], []
    for base,_,files in os.walk(ddir):
        for f in files:
            rel = os.path.relpath(os.path.join(base,f), root).replace("\\","/")
            low=f.lower()
            if low.endswith((".arrow",".ipc",".feather")): arrow.append(rel)
            elif args.include_parquet and low.endswith(".parquet"): parquet.append(rel)
    os.makedirs(ddir, exist_ok=True)
    out = {"arrow": sorted(arrow)}
    if args.include_parquet: out["parquet"] = sorted(parquet)
    with open(os.path.join(ddir,"manifest.json"),"w",encoding="utf-8") as fp: json.dump(out, fp, indent=2)
    print(f"[manifest] wrote {len(arrow)} arrow file(s), {len(parquet)} parquet file(s)")

if __name__ == "__main__":
    main()
