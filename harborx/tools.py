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

def tidy_repo(apply:bool=False, aggressive:bool=False):
    plan = []

    def mv(src, dst):
        if os.path.exists(src):
            plan.append(("move", src, dst))

    def rm(path):
        if os.path.exists(path):
            plan.append(("remove", path))

    # apps/web & docs
    mv("poc-e2e-blob-sql/packages/web-demo", "apps/web")
    mv("poc-e2e-blob-sql/packages/docs", "docs")

    # bench/legacy
    if aggressive:
        mv("scripts", "bench/sqlite-synthetic")
        mv("poc-sqlite-benchmark", "bench/sqlite-bench")
    else:
        mv("scripts", "legacy/scripts")
        mv("poc-sqlite-benchmark", "legacy/sqlite-benchmark")

    # misc legacy buckets
    mv("web", "legacy/web")
    mv("data", "legacy/data")
    mv("lake", "legacy/lake")
    mv("poc-e2e-blob-sql/packages/ingestor", "legacy/ingestor")

    # nested mistake cleanup
    nested = "poc-e2e-blob-sql/packages/web-demo/poc-e2e-blob-sql"
    if os.path.exists(nested):
        plan.append(("remove", nested))

    print("=== Proposed repo layout ===")
    print("harborx/                 # unified CLI + data channel")
    print("apps/web/                # pure-frontend demo (DuckDB-WASM)")
    print("docs/                    # docs")
    print("bench/                   # benchmarks (optional)")
    print("legacy/                  # archived code")
    print(".gitignore               # ignore data/")
    print("pyproject.toml           # entrypoint: harborx")
    print()

    for op, a, b in plan:
        if op == "move":
            print(f" - MOVE  {a}  ->  {b}")
        else:
            print(f" - RM    {a}")

    if not apply:
        print("\n(dry-run) Nothing changed. To apply:\n  harborx tidy --apply [--aggressive]")
        return

    for op, a, b in plan:
        if op == "move":
            os.makedirs(os.path.dirname(b), exist_ok=True)
            if os.path.exists(b):
                base = os.path.basename(a.rstrip('/\\'))
                dst = os.path.join(b, f"_migrated_{base}")
                print(f"   ! {b} exists, moving into {dst}")
                shutil.move(a, dst)
            else:
                shutil.move(a, b)
        elif op == "remove":
            if os.path.isdir(a):
                shutil.rmtree(a)
            else:
                os.remove(a)
    print("\n[tidy] repo re-organization complete.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--aggressive", action="store_true")
    args = ap.parse_args()
    tidy_repo(apply=args.apply, aggressive=args.aggressive)
