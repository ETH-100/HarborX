#!/usr/bin/env python3
# compact_arrow.py -- LWW-compact Arrow files (subset by blob index) into a single Arrow snapshot
import argparse, os, time
from pathlib import Path
import duckdb, pyarrow as pa, pyarrow.ipc as ipc

def write_empty_snapshot(out_path:str, schema):
    tmp = out_path + ".tmp"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with pa.OSFile(tmp, "wb") as sink:
        with ipc.RecordBatchFileWriter(sink, schema) as w:
            pass  # write zero batches, schema-only file
    os.replace(tmp, out_path)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arrowdir", default="lake/hot")
    ap.add_argument("--out", default="lake/base/state_current.arrow")
    ap.add_argument("--max-index", type=int, help="only include blobs with index <= this (optional)")
    args = ap.parse_args()

    all_paths = sorted([str(p).replace("\\","/") for p in Path(args.arrowdir).glob("*.arrow")])
    if not all_paths:
        raise SystemExit(f"No .arrow in {args.arrowdir}")

    if args.max_index is not None:
        paths = [p for p in all_paths if int(Path(p).stem.split("_")[-1]) <= args.max_index]
    else:
        paths = all_paths

    # If no base files selected (e.g., max-index == 0), write an EMPTY snapshot with the same schema
    if not paths:
        schema = ipc.open_file(all_paths[0]).schema  # infer schema from any file
        t0=time.time()
        write_empty_snapshot(args.out, schema)
        dt=time.time()-t0
        print(f"[compact-arrow] wrote {args.out} rows=0 in {dt:.3f}s (empty base)")
        return

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    con = duckdb.connect()
    views = []
    for i,p in enumerate(paths):
        tbl = ipc.open_file(p).read_all()
        name = f"t{i}"
        con.register(name, tbl)
        views.append(f"SELECT * FROM {name}")
    con.execute("CREATE OR REPLACE VIEW state AS " + " UNION ALL ".join(views))
    sql = """
    WITH ranked AS (
      SELECT key, value, timestamp, blob_index, position, type, address, tx_hash,
             ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC, blob_index DESC, position DESC) rn
      FROM state
    )
    SELECT type,address,key,value,tx_hash,blob_index,position,timestamp
    FROM ranked WHERE rn=1;
    """
    t0=time.time()
    out_tbl = con.execute(sql).fetch_arrow_table()
    dt=time.time()-t0
    tmp = args.out + ".tmp"
    with pa.OSFile(tmp, "wb") as sink:
        with ipc.RecordBatchFileWriter(sink, out_tbl.schema) as w:
            w.write_table(out_tbl)
    os.replace(tmp, args.out)
    print(f"[compact-arrow] wrote {args.out} rows={out_tbl.num_rows} in {dt:.3f}s")

if __name__ == "__main__":
    main()
