#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Dict, Any, Iterable, List
import pandas as pd

TABLES = ("storage_diffs","declared_classes","deployed_or_replaced","nonces")

def read_json_candidates(p: Path):
    txt = p.read_text(encoding="utf-8").strip()
    if not txt: return
    if txt[0] == "{":
        yield json.loads(txt)
    elif txt[0] == "[":
        for x in json.loads(txt):
            if isinstance(x, dict): yield x
    else:
        for line in txt.splitlines():
            line = line.strip()
            if line: yield json.loads(line)

def write_primary_manifest(out_root: Path, manifest_path: Path, primary="storage_diffs"):
    rel_prefix = "data/local/state_diff"
    src_dir = out_root / primary
    files = []
    if src_dir.exists():
        for f in sorted(src_dir.glob("*.parquet")):
            files.append(f"{rel_prefix}/{primary}/{f.name}".replace("\\", "/"))
    manifest = {"arrow": [], "parquet": files, "files": [], "segments": []}
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    if manifest_path.exists():
        import time, shutil
        backup_dir = manifest_path.parent / "_backups"
        backup_dir.mkdir(exist_ok=True)
        ts = time.strftime("%Y%m%d-%H%M%S")
        shutil.copyfile(manifest_path, backup_dir / f"manifest.{ts}.json")
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ingest] wrote primary manifest → {manifest_path} (files={len(files)})")

def norm_hex(x):
    if x is None: return None
    s = str(x)
    if s.startswith("0x"):
        s = "0x" + s[2:].lower().lstrip("0")
        return s if s != "0x" else "0x0"
    return s

def extract_frames(objs: Iterable[Dict[str, Any]], src_name: str):
    rows = {t: [] for t in TABLES}
    for o in objs:
        for it in (o.get("storage_diffs") or []):
            addr = norm_hex(it.get("address"))
            for se in (it.get("storage_entries") or []):
                rows["storage_diffs"].append({
                    "address": addr,
                    "key": norm_hex(se.get("key")),
                    "value": norm_hex(se.get("value")),
                    "src": src_name,
                })
        for dc in (o.get("declared_classes") or []):
            rows["declared_classes"].append({
                "class_hash": norm_hex(dc.get("class_hash")),
                "compiled_class_hash": norm_hex(dc.get("compiled_class_hash")),
                "src": src_name,
            })
        for d in (o.get("deployed_or_replaced") or []):
            rows["deployed_or_replaced"].append({
                "address": norm_hex(d.get("address")),
                "class_hash": norm_hex(d.get("class_hash")),
                "src": src_name,
            })
        for n in (o.get("nonces") or []):
            rows["nonces"].append({
                "contract_address": norm_hex(n.get("contract_address")),
                "nonce": norm_hex(n.get("nonce")),
                "src": src_name,
            })
    def df_for(name):
        data = rows[name]
        if not data: return pd.DataFrame(columns=[], dtype="string")
        df = pd.DataFrame(data)
        for c in df.columns: df[c] = df[c].astype("string")
        return df
    return {name: df_for(name) for name in TABLES}

def safe_write_parquet(df: pd.DataFrame, out_dir: Path, out_name: str):
    import pyarrow as pa, pyarrow.parquet as pq
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   out_dir/out_name, compression="zstd", use_dictionary=True)

def sha1sum(p: Path) -> str:
    import hashlib
    h = hashlib.sha1()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1<<16), b""): h.update(chunk)
    return h.hexdigest()

def load_ledger(ledger_path: Path) -> Dict[str, Any]:
    if not ledger_path.exists(): return {"files": {}}
    try: return json.loads(ledger_path.read_text(encoding="utf-8"))
    except: return {"files": {}}

def save_ledger(ledger_path: Path, data: Dict[str, Any]):
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = ledger_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(ledger_path)

def build_tables_json(out_root: Path, rel_prefix: str = "data/local/state_diff") -> Dict[str, Any]:
    tables = {}
    for t in TABLES:
        dirp = out_root / t
        files = []
        if dirp.exists():
            for f in sorted(dirp.glob("*.parquet")):
                files.append(f"{rel_prefix}/{t}/{f.name}".replace("\\","/"))
        tables[t] = files
    return tables

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=".cache", help="folder with decoded JSON")
    ap.add_argument("--out", default="apps/web/data/local/state_diff", help="output root")
    ap.add_argument("--ledger", default="apps/web/data/local/state_diff/_processed.json")
    ap.add_argument("--force", action="store_true", help="reprocess all")
    args = ap.parse_args()

    src, out, ledger_path = Path(args.src), Path(args.out), Path(args.ledger)
    files = sorted(list(src.glob("*.json")) + list(src.glob("*.JSON"))
             + list(src.glob("*.txt"))  + list(src.glob("*.TXT")))
    if not files:
        print(f"[ingest] no json files in {src}"); return

    ledger = load_ledger(ledger_path)
    processed = ledger.get("files", {})
    changed = 0

    for p in files:
        digest = sha1sum(p)
        rec = processed.get(p.name)
        if (not args.force) and rec and rec.get("sha1")==digest:
            continue
        print(f"[ingest] processing {p.name} …")
        objs = list(read_json_candidates(p))
        frames = extract_frames(objs, p.name)
        wrote_any = False
        for t, df in frames.items():
            if df is None or df.empty:
                continue
            out_dir = out / t
            if args.split_rows and len(df) > args.split_rows:
                k = 0
                for i in range(0, len(df), args.split_rows):
                    part = df.iloc[i:i+args.split_rows].copy()
                    out_name = f"{p.stem}-{digest[:8]}-{k:03d}.parquet"
                    safe_write_parquet(part, out_dir, out_name)
                    print(f"  → {out_dir/out_name} rows={len(part)}")
                    k += 1
            else:
                out_name = f"{p.stem}-{digest[:8]}.parquet"
                safe_write_parquet(df, out_dir, out_name)
                print(f"  → {out_dir/out_name} rows={len(df)}")

        processed[p.name] = {"sha1": digest, "written": wrote_any}
        changed += 1

    ledger["files"] = processed
    save_ledger(ledger_path, ledger)

    tables = build_tables_json(out)

    primary_manifest = (out.parent / "manifest.json")  # apps/web/data/local/manifest.json
    write_primary_manifest(out, primary_manifest, primary="storage_diffs")

    print(f"[ingest] done. changed={changed}, total={len(files)}")

if __name__ == "__main__":
    main()
