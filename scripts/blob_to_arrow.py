#!/usr/bin/env python3
import argparse, gzip, os, time
import pyarrow as pa, pyarrow.ipc as ipc
def parse_blob(path:str, chunk:int=200_000):
    with gzip.open(path, "rb") as gz:
        if gz.read(4) != b"ZKBL": raise ValueError("Bad magic")
        gz.read(2); gz.read(2)      # version, flags
        gz.read(4)                  # batch_id
        ts = int.from_bytes(gz.read(8), "big")
        n = int.from_bytes(gz.read(4), "big")
        rec_len = int.from_bytes(gz.read(2), "big")
        if rec_len != 126: raise ValueError(f"Unexpected rec_len {rec_len}")
        for off in range(0, n, chunk):
            m = min(chunk, n-off)
            typ, addr, key, val, txh, bidx, pos, ts_list = [],[],[],[],[],[],[],[]
            for _ in range(m):
                rtype = gz.read(1)[0]; gz.read(1)
                address = gz.read(20).hex()
                k = gz.read(32).hex(); v = gz.read(32).hex(); th = gz.read(32).hex()
                blob_index = int.from_bytes(gz.read(4), "big")
                p = int.from_bytes(gz.read(4), "big")
                typ.append("state_diff" if rtype==2 else "tx")
                addr.append(address); key.append(k); val.append(v); txh.append(th)
                bidx.append(blob_index); pos.append(p); ts_list.append(ts)
            yield pa.record_batch({
                "type": pa.array(typ, type=pa.string()),
                "address": pa.array(addr, type=pa.string()),
                "key": pa.array(key, type=pa.string()),
                "value": pa.array(val, type=pa.string()),
                "tx_hash": pa.array(txh, type=pa.string()),
                "blob_index": pa.array(bidx, type=pa.uint32()),
                "position": pa.array(pos, type=pa.uint32()),
                "timestamp": pa.array(ts_list, type=pa.uint64()),
            })
def atomic_replace(src, dst, retries=12, backoff=0.2):
    for i in range(retries):
        try:
            os.replace(src, dst); return
        except PermissionError: time.sleep(backoff * (i+1))
    raise
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blob", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--chunk", type=int, default=200_000)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    gen = parse_blob(args.blob, args.chunk)
    first = next(gen, None)
    if first is None: raise SystemExit("empty blob")
    tmp = args.out + ".tmp"
    with pa.OSFile(tmp, "wb") as sink:
        with ipc.RecordBatchFileWriter(sink, first.schema) as w:
            w.write_batch(first)
            for b in gen: w.write_batch(b)
    atomic_replace(tmp, args.out)
    print(f"[blob->arrow] wrote {args.out}")
if __name__ == "__main__": main()
