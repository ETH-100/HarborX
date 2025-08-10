from __future__ import annotations
import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from .decoder import parse_blob
import pyarrow as pa
import pyarrow.compute as pc

def _add_partition_columns(batch: pa.RecordBatch, chain_id: int):
    ts_i64 = pc.cast(batch.column("timestamp"), pa.int64())

    days = pc.floor(pc.divide(ts_i64, pa.scalar(86400, type=pa.int64())))
    date_part = pc.cast(days, pa.int32()) 

    chain_ids = pa.array([int(chain_id)] * batch.num_rows, type=pa.int64())
    topic_arr = batch.column("type")

    cols   = list(batch.schema.names) + ["chain_id", "date", "topic"]
    arrays = list(batch.columns)       + [chain_ids, date_part, topic_arr]
    return pa.RecordBatch.from_arrays(arrays, names=cols)

def ingest_blob_to_dataset(blob_path: str, out_dir: str, chain_id: int, max_row_group:int=8192):
    os.makedirs(out_dir, exist_ok=True)
    for batch in parse_blob(blob_path, chunk=max_row_group):
        b = _add_partition_columns(batch, chain_id)
        table = pa.Table.from_batches([b])
        pq.write_to_dataset(
            table,
            root_path=out_dir,
            partition_cols=["chain_id","date","topic"],
            use_dictionary=True,
            compression="zstd",
        )

def ingest_folder(source_dir: str, out_dir: str, chain_id: int, max_row_group:int=8192):
    blobs = [p for p in sorted(os.listdir(source_dir)) if p.endswith(".blob.gz")]
    if not blobs:
        raise SystemExit(f"No .blob.gz found in {source_dir}")
    for i, name in enumerate(blobs, 1):
        path = os.path.join(source_dir, name)
        print(f"[ingest] ({i}/{len(blobs)}) {path}")
        ingest_blob_to_dataset(path, out_dir, chain_id, max_row_group=max_row_group)
    print(f"[ingest] DONE → {out_dir}")
