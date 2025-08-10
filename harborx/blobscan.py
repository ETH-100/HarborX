from __future__ import annotations
import os, time, json, datetime as dt
from typing import Any, Dict, List, Optional
import requests
import pyarrow as pa, pyarrow.ipc as ipc, pyarrow.parquet as pq

def ts_date(ts:int)->str:
    return dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")

def ensure_dir(p:str): os.makedirs(p, exist_ok=True)

def write_table(out_dir:str, chain_id:int, ts:int, topic:str, table:pa.Table, write_parquet:bool):
    base = os.path.join(out_dir, f"chain_id={chain_id}", f"date={ts_date(ts)}", f"topic={topic}")
    ensure_dir(base)
    stem = f"part-{int(time.time()*1000)}"
    arrow_path = os.path.join(base, stem + ".arrow")
    with pa.OSFile(arrow_path, "wb") as sink:
        with ipc.RecordBatchFileWriter(sink, table.schema) as w:
            for b in table.to_batches(max_chunksize=8192):
                w.write_batch(b)
    parquet_path=None
    if write_parquet:
        parquet_path = os.path.join(base, stem + ".parquet")
        pq.write_table(table, parquet_path, compression="snappy")
    return arrow_path, parquet_path

def build_manifest(root:str, data:str="data", include_parquet:bool=False):
    data_dir = os.path.join(root, data)
    arrow, parquet = [], []
    for r,_,files in os.walk(data_dir):
        for f in files:
            rel=os.path.relpath(os.path.join(r,f), data_dir).replace("\\","/")
            lf=f.lower()
            if lf.endswith((".arrow",".ipc",".feather")): arrow.append(rel)
            elif include_parquet and lf.endswith(".parquet"): parquet.append(rel)
    os.makedirs(data_dir, exist_ok=True)
    with open(os.path.join(data_dir,"manifest.json"),"w",encoding="utf-8") as fp:
        json.dump({"arrow":sorted(arrow), **({"parquet":sorted(parquet)} if include_parquet else {})}, fp, indent=2)
    print(f"[manifest] wrote {len(arrow)} arrow file(s){' and '+str(len(parquet))+' parquet file(s)' if include_parquet else ''} at {data_dir}")

def _get_json(session:requests.Session, url:str)->Any:
    r = session.get(url, timeout=30, headers={"accept":"application/json","user-agent":"harborx/0.1"})
    r.raise_for_status()
    return r.json()

def _get_bytes(session:requests.Session, url:str)->bytes:
    r = session.get(url, timeout=60, headers={"accept":"application/octet-stream,application/json","user-agent":"harborx/0.1"})
    r.raise_for_status()
    return r.content

def _parse_list_payload(j:Any)->list:
    if isinstance(j, list): return j
    if isinstance(j, dict):
        for k in ("items","data","blobs","results"):
            v = j.get(k)
            if isinstance(v, list): return v
        if all(isinstance(v, dict) for v in j.values()):
            return list(j.values())
    raise ValueError("Unrecognized list payload shape")

def list_blobs(base:str, limit:int, offset:int, debug:bool=False)->List[Dict[str,Any]]:
    session = requests.Session()
    tried = []
    url1 = f"{base.rstrip('/')}/blobs?limit={limit}&offset={offset}"
    try:
        j = _get_json(session, url1)
        return _parse_list_payload(j)[:limit]
    except Exception as e1:
        tried.append((url1, e1))

    url2 = f"{base.rstrip('/')}/blobs"
    try:
        j = _get_json(session, url2)
        return _parse_list_payload(j)[:limit]
    except Exception as e2:
        tried.append((url2, e2))

    msg = ["Blobscan listing failed; tried:"]
    for u, err in tried:
        msg.append(f"  - {u} -> {type(err).__name__}: {err}")
    if debug:
        print("\n".join(msg))
    raise SystemExit("\n".join(msg))

def get_blob_meta(base:str, blob_id_or_vh:str)->Dict[str,Any]:
    session = requests.Session()
    url = f"{base.rstrip('/')}/blobs/{blob_id_or_vh}"
    return _get_json(session, url)

def get_blob_data(base:str, blob_id_or_vh:str)->bytes:
    session = requests.Session()
    url = f"{base.rstrip('/')}/blobs/{blob_id_or_vh}/data"
    return _get_bytes(session, url)

def pick_id(meta:Dict[str,Any])->str:
    for k in ("id","blobId","blob_id","versionedHash","versioned_hash","vh"):
        v = meta.get(k)
        if v: return str(v)
    b = meta.get("blob") or {}
    for k in ("id","versionedHash","versioned_hash"):
        v = b.get(k)
        if v: return str(v)
    raise KeyError("no blob id/versionedHash in metadata")

def extract_tx_hash(meta:Dict[str,Any])->Optional[bytes]:
    def _hb(v):
        v = v if isinstance(v, str) else ""
        try:
            h = v[2:] if v.startswith("0x") else v
            return bytes.fromhex(h) if len(h) >= 64 else None
        except Exception:
            return None
    for k in ("transactionHash","txHash","hash"):
        v = meta.get(k) or (meta.get("tx") or {}).get(k)
        h = _hb(v)
        if h: return h
    for k in ("versionedHash","versioned_hash","vh"):
        v = meta.get(k) or (meta.get("blob") or {}).get(k)
        h = _hb(v)
        if h: return h
    return None

def extract_to_address(meta:Dict[str,Any]):
    for path in (("to",),("tx","to"),("transaction","to")):
        m = meta
        for p in path:
            if not isinstance(m, dict): break
            m = m.get(p)
        if isinstance(m, str):
            return m.lower()
    return None

def extract_timestamp(meta:Dict[str,Any])->int:
    import datetime as _dt, time as _t
    for path in (("timestamp",),("time",),("blockTimestamp",),("tx","timestamp"),("transaction","timestamp")):
        m = meta
        for p in path:
            if not isinstance(m, dict): break
            m = m.get(p)
        if isinstance(m, (int,float)): return int(m)
        if isinstance(m, str):
            try:
                if m.isdigit(): return int(m)
                return int(_dt.datetime.fromisoformat(m.replace("Z","+00:00")).timestamp())
            except Exception:
                pass
    return int(_t.time())

def rows_from_blob(raw:bytes, tx_hash, ts:int, blob_index:int):
    n=len(raw)//32; rows=[]
    for i in range(n):
        rows.append({"type":"raw","address":None,"key":None,"value":raw[i*32:(i+1)*32],
                     "tx_hash": tx_hash or b"\\x00"*32,"blob_index":blob_index,"position":i,"timestamp":ts})
    return rows

def batch_from_rows(rows:list)->pa.RecordBatch:
    return pa.record_batch({
        "type": pa.array([r["type"] for r in rows], type=pa.string()),
        "address": pa.array([r["address"] for r in rows], type=pa.binary(20)),
        "key": pa.array([r["key"] for r in rows], type=pa.binary(32)),
        "value": pa.array([r["value"] for r in rows], type=pa.binary(32)),
        "tx_hash": pa.array([r["tx_hash"] for r in rows], type=pa.binary(32)),
        "blob_index": pa.array([r["blob_index"] for r in rows], type=pa.uint32()),
        "position": pa.array([r["position"] for r in rows], type=pa.uint32()),
        "timestamp": pa.array([r["timestamp"] for r in rows], type=pa.uint64()),
    })

def run_blobscan_fetch(base:str, limit:int, offset:int, out_dir:str, chain_id:int, write_parquet:bool, core_addr:str, debug:bool=False):
    blobs = list_blobs(base, limit=limit, offset=offset, debug=debug)

    picked = []
    for m in blobs:
        try:
            to = (extract_to_address(m) or "").lower()
            if core_addr and to and to != core_addr:
                continue
            picked.append(m)
        except Exception:
            picked.append(m)

    if not picked:
        raise SystemExit("No blobs passed the filter; try removing --core or increase --limit/--offset")

    written=0
    for meta in picked[:limit]:
        try:
            blob_id = pick_id(meta)
        except Exception:
            blob_id = meta.get("versionedHash") or (meta.get("blob") or {}).get("versionedHash")
            if not blob_id:
                if debug: print(f"[blobscan] skip item without id/versionedHash: {meta}")
                continue

        try:
            full = get_blob_meta(base, blob_id) or {}
            if isinstance(full, dict) and len(full) >= len(meta):
                meta = {**meta, **full}
        except Exception as e:
            if debug: print(f"[blobscan] meta fetch failed for {blob_id}: {e}")

        raw = get_blob_data(base, blob_id)
        txh = extract_tx_hash(meta)
        ts  = extract_timestamp(meta)
        bidx = int(meta.get("index") or meta.get("blobIndex") or meta.get("blob_index") or 0)

        rows = rows_from_blob(raw, txh, ts, bidx)
        table = pa.Table.from_batches([batch_from_rows(rows)])
        write_table(out_dir, chain_id, ts, "raw", table, write_parquet)
        written+=1

    if written==0:
        raise SystemExit("Could not write any blob rows (no data fetched).")

    build_manifest(root=os.path.dirname(out_dir), data=os.path.basename(out_dir), include_parquet=write_parquet)
    print(f"[blobscan] DONE wrote {written} blob file(s) -> {out_dir}")
