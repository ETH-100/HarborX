#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, json

def build_manifest(root:str="apps/web", data:str="data", include_parquet:bool=False):
    """
    Scan the web data directory and produce a simple manifest.json.
    - Always collects Arrow/IPC/Feather files into `arrow`.
    - If include_parquet=True, also collects `.parquet` into `parquet`.
    """
    # Lazy import to keep base install light
    import pyarrow as pa  # noqa: F401
    import pyarrow.ipc as ipc  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401

    arrow, parquet = [], []
    data_dir = os.path.join(root, data)
    for base, _, files in os.walk(data_dir):
        for f in files:
            rel = os.path.relpath(os.path.join(base,f), data_dir).replace("\\","/")
            lf = f.lower()
            if lf.endswith((".arrow",".ipc",".feather")):
                arrow.append(f"{data}/{rel}")
            elif include_parquet and lf.endswith(".parquet"):
                parquet.append(f"{data}/{rel}")
    os.makedirs(data_dir, exist_ok=True)
    manifest = {"arrow": sorted(arrow)}
    if include_parquet:
        manifest["parquet"] = sorted(parquet)
    with open(os.path.join(data_dir,"manifest.json"),"w",encoding="utf-8") as fp:
        json.dump(manifest, fp, indent=2)
    print(f"[manifest] wrote {len(arrow)} arrow file(s){' and '+str(len(parquet))+' parquet file(s)' if include_parquet else ''} at {data_dir}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="HarborX tools")
    ap.add_argument("--root", default="apps/web")
    ap.add_argument("--data", default="data")
    ap.add_argument("--include-parquet", action="store_true")
    args = ap.parse_args()
    build_manifest(root=args.root, data=args.data, include_parquet=args.include_parquet)
