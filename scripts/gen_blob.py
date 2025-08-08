#!/usr/bin/env python3
import argparse, os, time, gzip, secrets, struct, random, hashlib

MAGIC = b"ZKBL"
REC_LEN = 1+1+20+32+32+32+4+4  # 126

def key_bytes_from_id(i:int)->bytes:
    return hashlib.sha256(f"K{i}".encode()).digest()

def addr_bytes_from_id(i:int)->bytes:
    import hashlib as _h
    return _h.sha1(f"A{i}".encode()).digest()  # 20 bytes

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="lake/blob", help="output prefix (without index)")
    ap.add_argument("--rows", type=int, default=1_000_000, help="rows per part")
    ap.add_argument("--parts", type=int, default=1, help="number of blob files")
    ap.add_argument("--blob-type", type=int, default=2, help="record type (1=tx,2=state_diff)")
    # update control
    ap.add_argument("--keyspace", type=int, help="global number of distinct keys; smaller => more updates")
    ap.add_argument("--hot-frac", type=float, default=0.05, help="fraction of keys in hot set")
    ap.add_argument("--hot-amp", type=float, default=20.0, help="hot set sampling multiplier vs cold")
    ap.add_argument("--seed", type=int, default=0, help="rng seed for reproducibility")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    ts = int(time.time())
    K = args.keyspace if args.keyspace and args.keyspace > 0 else None
    if K:
        hotK = max(1, int(K * max(0.0, min(1.0, args.hot_frac))))
        coldK = max(1, K - hotK)
        p_hot = (args.hot_amp * hotK) / (args.hot_amp * hotK + coldK)
    else:
        hotK = coldK = 0
        p_hot = 0.0

    for i in range(1, args.parts+1):
        path = f"{args.out}_{i:06d}.blob.gz"
        tmp = path + ".tmp"
        print(f"[gen-blob] writing {path} rows={args.rows:,}"+(f" keyspace={K:,}" if K else ""))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with gzip.open(tmp, "wb") as gz:
            gz.write(MAGIC)
            gz.write(struct.pack(">H", 1))
            gz.write(struct.pack(">H", 0))
            gz.write(struct.pack(">I", i))
            gz.write(struct.pack(">Q", ts))
            gz.write(struct.pack(">I", args.rows))
            gz.write(struct.pack(">H", REC_LEN))
            for pos in range(args.rows):
                rec = bytearray()
                rec += struct.pack("B", args.blob_type)
                rec += b"\x00"
                if K:
                    if rng.random() < p_hot:
                        kid = rng.randrange(0, hotK)
                    else:
                        kid = hotK + rng.randrange(0, coldK)
                    key = key_bytes_from_id(kid)
                    address = addr_bytes_from_id(kid)
                else:
                    key = secrets.token_bytes(32)
                    address = secrets.token_bytes(20)
                value = secrets.token_bytes(32)
                txh = secrets.token_bytes(32)
                rec += address
                rec += key
                rec += value
                rec += txh
                rec += struct.pack(">I", i)
                rec += struct.pack(">I", pos)
                gz.write(rec)
        os.replace(tmp, path)
        print(f"[gen-blob] done {path}")

if __name__ == "__main__":
    main()
