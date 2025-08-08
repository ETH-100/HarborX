#!/usr/bin/env python3
import argparse, os, time, gzip, secrets, struct, random

MAGIC = b"ZKBL"
REC_LEN = 1+1+20+32+32+32+4+4  # 126

def rand_bytes(n:int)->bytes:
    return secrets.token_bytes(n)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="lake/blob", help="output prefix (without index)")
    ap.add_argument("--rows", type=int, default=1_000_000, help="rows per part")
    ap.add_argument("--parts", type=int, default=1, help="number of blob files")
    ap.add_argument("--blob-type", type=int, default=2, help="record type (1=tx,2=state_diff)")
    ap.add_argument("--update-ratio", type=float, default=0.0, help="0..1, ratio of updates (repeated keys)")
    ap.add_argument("--keys", type=int, help="number of unique (address,key). Default = rows*(1-update_ratio)")
    ap.add_argument("--seed", type=int, help="optional RNG seed for reproducibility")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    ts = int(time.time())
    for i in range(1, args.parts+1):
        path = f"{args.out}_{i:06d}.blob.gz"
        tmp = path + ".tmp"
        os.makedirs(os.path.dirname(path), exist_ok=True)

        keys = args.keys if args.keys else max(1, int(args.rows * (1.0 - args.update_ratio)))
        addr_pool = [rand_bytes(20) for _ in range(keys)]
        key_pool  = [rand_bytes(32) for _ in range(keys)]

        print(f"[gen-blob] writing {path} rows={args.rows:,} keys={keys:,} update_ratio~={args.update_ratio:.2f}")
        with gzip.open(tmp, "wb") as gz:
            # header
            gz.write(MAGIC)
            gz.write(struct.pack(">H", 1))      # version
            gz.write(struct.pack(">H", 0))      # flags
            gz.write(struct.pack(">I", i))      # batch id
            gz.write(struct.pack(">Q", ts))     # timestamp
            gz.write(struct.pack(">I", args.rows)) # nrecords
            gz.write(struct.pack(">H", REC_LEN))   # record length
            # records
            for pos in range(args.rows):
                # first 'keys' rows create first versions; remaining rows pick a random existing key to update
                if pos < keys:
                    idx = pos
                else:
                    idx = random.randrange(keys)

                rec = bytearray()
                rec += struct.pack("B", args.blob_type)  # type
                rec += b"\x00"                           # reserved
                rec += addr_pool[idx]                    # address
                rec += key_pool[idx]                     # key
                rec += rand_bytes(32)                    # value (changes across versions)
                rec += rand_bytes(32)                    # tx_hash
                rec += struct.pack(">I", i)              # blob_index
                rec += struct.pack(">I", pos)            # position (monotonic in this batch)
                gz.write(rec)
        os.replace(tmp, path)
        updates = max(0, args.rows - keys)
        print(f"[gen-blob] done {path} (unique={keys:,}, updates~={updates:,})")

if __name__ == "__main__":
    main()
