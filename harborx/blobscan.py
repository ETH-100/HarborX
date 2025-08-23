from __future__ import annotations
import os, sys, json, time, subprocess, traceback
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict
import requests

def _write_txt(base: str, chain_id: int, eth_block: int, idx: int, content: bytes, ts: int) -> str:
    outdir = os.path.join(base, f"chain_id={chain_id}", f"date={_ts_utc_str(ts)}", "topic=raw_txt")
    ensure_dir(outdir)
    path = os.path.join(outdir, f"part-{int(time.time()*1000)}.txt")
    with open(path, "wb") as fp:
        fp.write(content) 
    return os.path.abspath(path)

def fetch_storage_bytes(url: str, debug: bool=False) -> bytes:
    if debug:
        print(f"[sn] storage bytes from {url} (expect 131072 bytes)")
    import requests
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

# ----------------------------- utils -----------------------------
def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def _ts_utc_str(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")

def _parse_ts(it: Dict[str, Any]) -> int:
    t = it.get("timestamp")
    if isinstance(t, (int, float)):
        return int(t)
    iso = it.get("blockTimestamp") or it.get("time")
    if isinstance(iso, str) and iso:
        try:
            iso2 = iso.replace("Z", "+00:00")
            return int(datetime.fromisoformat(iso2).timestamp())
        except Exception:
            pass
    return int(time.time())

def _pick_storage_url(it: Dict[str, Any]) -> Optional[str]:
    refs = it.get("dataStorageReferences") or it.get("data_storage_references")
    if isinstance(refs, list) and refs:
        for r in refs:
            u = (r or {}).get("url")
            if isinstance(u, str) and u:
                return u
    st = it.get("storage") or {}
    if isinstance(st, dict):
        u = st.get("url")
        if isinstance(u, str) and u:
            return u
    u = it.get("storageUrl")
    if isinstance(u, str) and u:
        return u
    return None

# ----------------------------- fetch blobs -----------------------------
BLOSCAN_API = "https://api.blobscan.com/blobs"

def fetch_starknet_blobs(page: int, page_size: int, start_block: int=0, end_block: int=0, debug: bool=False) -> List[Dict[str, Any]]:
    params = {
        "p": page, "ps": page_size, "count": "false", "sort": "desc",
        "rollups": "starknet", "category": "rollup", "type": "canonical"
    }
    if start_block:
        params["startBlock"] = start_block
    if end_block:
        params["endBlock"] = end_block

    if debug:
        qp = "&".join(f"{k}={v}" for k,v in params.items())
        print(f"[sn] GET {BLOSCAN_API}?{qp}")
    r = requests.get(BLOSCAN_API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    items = data.get("items")
    if not isinstance(items, list):
        items = data.get("blobs")
    if not isinstance(items, list):
        if isinstance(data, list):
            items = data
        else:
            items = []

    if debug:
        print(f"[sn] got {len(items)} items")
    return items

# ----------------------------- manifest & config -----------------------------
def build_grouped_manifest(entries: List[Tuple[int,str,int,str,int]], root_out: str) -> str:
    """
    entries: [(eth_block, tx_hash, idx, abs_path, ts), ...]
    manifest schema:
    { "entries": [ { "eth_block": u64, "starknet_block": 0, "blobs":[{"path": "..."}...] }, ... ] }
    """
    groups: Dict[Tuple[int,str], List[Tuple[int,str]]] = defaultdict(list)
    for eb, txh, idx, pth, ts in entries:
        groups[(eb, txh)].append((idx, pth))

    manifest_entries = []
    for (eb, _txh), lst in groups.items():
        lst.sort(key=lambda t: t[0])  # by idx
        idxs = [i for i,_ in lst]
        if not idxs or idxs != list(range(idxs[-1] + 1)):
            continue
        manifest_entries.append({
            "eth_block": int(eb),
            "starknet_block": 0,
            "blobs": [{"path": os.path.abspath(p)} for _, p in lst],
        })

    root = os.path.dirname(root_out.rstrip("\\/"))
    path = os.path.join(root, "decoder_manifest.json")
    with open(path, "w", encoding="utf-8") as fp:
        json.dump({"entries": manifest_entries}, fp, indent=2)
    return os.path.abspath(path)

def ensure_decoder_config(config_path: str, cache_dir: str) -> str:
    ensure_dir(cache_dir)
    if os.path.exists(config_path):
        return os.path.abspath(config_path)
    norm_cache = cache_dir.replace("\\", "/")
    norm_db    = os.path.join(cache_dir, "decoder.sqlite").replace("\\", "/")
    toml = (
        'rpc_url = "http://127.0.0.1:8545"\n'
        'blob_url_base = "http://127.0.0.1:12345/blobs"\n'
        f'cache_dir = "{norm_cache}"\n'
        f'db_file = "{norm_db}"\n'
        'pathfinder_rpc_url = "http://127.0.0.1:9545"\n'
    )
    with open(config_path, "w", encoding="utf-8") as fp:
        fp.write(toml)
    return os.path.abspath(config_path)

def _default_decoder_path() -> str:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    exe = "starknet-scrape.exe" if os.name == "nt" else "starknet-scrape"
    return os.path.join(repo_root, "tools", exe)

def run_external_decoder(decoder_path: str, config_path: str, manifest_path: str, debug: bool=False) -> int:
    if not decoder_path:
        decoder_path = _default_decoder_path()
    if not os.path.exists(decoder_path):
        raise FileNotFoundError(f"decoder not found: {decoder_path}")
    cmd = [
        decoder_path,
        "--config-file", config_path,
        "--manifest", manifest_path,
        "--parse", "--json", "--save"
    ]
    if debug:
        print("[sn] exec:", " ".join(cmd))
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if debug or proc.returncode != 0:
        print(proc.stdout)
    return proc.returncode

def list_new_json(cache_dir: str) -> List[str]:
    outs = []
    if not os.path.isdir(cache_dir):
        return outs
    for name in os.listdir(cache_dir):
        if name.endswith(".json"):
            outs.append(os.path.join(cache_dir, name))
    outs.sort()
    return outs

# ----------------------------- main pipeline -----------------------------
def sn_pipeline(out_dir: str, chain_id: int=1, page: int=1, page_size: int=50,
                max_items: int=20, start_block: int=0, end_block: int=0,
                storage_only: bool=False, download_only: bool=False,
                decoder_path: str="", decoder_config: str="", decoder_cache: str="",
                flip_endian: bool=False, debug: bool=False) -> None:
    """
    Download blobs -> write .bin -> grouped manifest (full sets only) ->
    call tools/starknet-scrape --manifest -> list JSON outputs.
    """
    items_api = fetch_starknet_blobs(page, page_size, start_block=start_block, end_block=end_block, debug=debug)
    if not items_api:
        print("[sn] no blobs returned")
        return

    items: List[Tuple[int,str,int,str,int]] = []  # (eth_block, tx_hash, idx, abs_path, ts)

    for it in items_api[:max_items]:
        eth_block = int(it.get("eth_block_number") or it.get("blockNumber") or 0)
        tx_hash   = it.get("tx_hash") or it.get("txHash") or ""
        idx       = int(it.get("index", 0))
        ts        = _parse_ts(it)
        storage_url = _pick_storage_url(it)
        if not storage_url:
            if debug: print(f"[sn] skip: no storage url for tx={tx_hash} idx={idx}")
            continue

        raw = fetch_storage_bytes(storage_url, debug=debug)
        p = _write_txt(out_dir, chain_id, eth_block, idx, raw, ts)
        print(f"[sn] wrote bin: {p} (block={eth_block}, index={idx})")
        items.append((eth_block, tx_hash, idx, p, ts))

    if not items:
        print("[sn] nothing written")
        return

    manifest_path = build_grouped_manifest(items, out_dir)
    print(f"[sn] manifest: {manifest_path}")

    if download_only:
        print("[sn] download only, skip decoding")
        return

    root_dir = os.path.dirname(out_dir.rstrip("\\/"))
    decoder_cache = decoder_cache or os.path.join(root_dir, "decoder_cache")
    ensure_dir(decoder_cache)
    config_path = decoder_config or os.path.join(root_dir, "decoder.toml")
    config_path = ensure_decoder_config(config_path, decoder_cache)

    rc = run_external_decoder(decoder_path, config_path, manifest_path, debug=debug)
    if rc != 0:
        print(f"[sn] external decoder exit code = {rc}")
        return

    outs = list_new_json(decoder_cache)
    if outs:
        print("[sn] JSON outputs:")
        for f in outs:
            print("   ", f)
    else:
        print("[sn] decoder finished but no JSON found under", decoder_cache)
