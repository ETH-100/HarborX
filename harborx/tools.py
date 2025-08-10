#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, json, shutil, sys

def build_manifest(root:str="apps/web", data:str="data", include_parquet:bool=False):
    import pyarrow as pa, pyarrow.ipc as ipc, pyarrow.parquet as pq  # noqa
    arrow, parquet = [], []
    data_dir = os.path.join(root, data)
    for base,_,files in os.walk(data_dir):
        for f in files:
            rel = os.path.relpath(os.path.join(base,f), data_dir).replace("\\","/")
            lf = f.lower()
            if lf.endswith((".arrow",".ipc",".feather")):
                arrow.append(f"{data}/{rel}")
            elif include_parquet and lf.endswith(".parquet"):
                parquet.append(f"{data}/{rel}")
    os.makedirs(data_dir, exist_ok=True)
    with open(os.path.join(data_dir,"manifest.json"),"w",encoding="utf-8") as fp:
        json.dump({"arrow":sorted(arrow), **({"parquet":sorted(parquet)} if include_parquet else {})}, fp, indent=2)
    print(f"[manifest] wrote {len(arrow)} arrow file(s){' and '+str(len(parquet))+' parquet file(s)' if include_parquet else ''} at {data_dir}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--aggressive", action="store_true")
    args = ap.parse_args()