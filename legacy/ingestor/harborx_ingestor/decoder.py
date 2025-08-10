from __future__ import annotations
import gzip
import pyarrow as pa

ADDR_T = pa.binary(20)
HASH_T = pa.binary(32)

SCHEMA = pa.schema([
    ("type", pa.string()),
    ("address", ADDR_T),
    ("key", HASH_T),
    ("value", HASH_T),
    ("tx_hash", HASH_T),
    ("blob_index", pa.uint32()),
    ("position", pa.uint32()),
    ("timestamp", pa.uint64()),
])

def parse_blob(path: str, chunk: int = 200_000):
    with gzip.open(path, "rb") as gz:
        if gz.read(4) != b"ZKBL":
            raise ValueError("Bad magic")
        gz.read(2); gz.read(2)      # version, flags
        gz.read(4)                  # batch id
        ts = int.from_bytes(gz.read(8), "big")
        n  = int.from_bytes(gz.read(4), "big")
        rec_len = int.from_bytes(gz.read(2), "big")
        if rec_len != 126:
            raise ValueError(f"Unexpected rec_len {rec_len}")
        for off in range(0, n, chunk):
            m = min(chunk, n-off)
            typ, addr, key, val, txh, bidx, pos, ts_list = [], [], [], [], [], [], [], []
            for _ in range(m):
                rtype = gz.read(1)[0]; gz.read(1)  # pad
                address = gz.read(20)
                k = gz.read(32)
                v = gz.read(32)
                th = gz.read(32)
                blob_index = int.from_bytes(gz.read(4), "big")
                p = int.from_bytes(gz.read(4), "big")
                typ.append("state_diff" if rtype==2 else "tx")
                addr.append(address); key.append(k); val.append(v); txh.append(th)
                bidx.append(blob_index); pos.append(p); ts_list.append(ts)
            yield pa.record_batch({
                "type": pa.array(typ, type=pa.string()),
                "address": pa.array(addr, type=pa.binary(20)),
                "key": pa.array(key, type=pa.binary(32)),
                "value": pa.array(val, type=pa.binary(32)),
                "tx_hash": pa.array(txh, type=pa.binary(32)),
                "blob_index": pa.array(bidx, type=pa.uint32()),
                "position": pa.array(pos, type=pa.uint32()),
                "timestamp": pa.array(ts_list, type=pa.uint64()),
            }, schema=SCHEMA)
