#!/usr/bin/env python3
# convert_parquet_to_arrow.py — Convenience: convert each .parquet under input dir to .arrow under output dir
# For demo-sized data (<200MB). Requires: pyarrow
import argparse, pathlib, os, pyarrow.parquet as pq, pyarrow as pa, pyarrow.ipc as ipc

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--inp', default='data', help='input directory containing .parquet')
    ap.add_argument('--out', default='data_arrow', help='output directory for .arrow')
    args = ap.parse_args()
    inp = pathlib.Path(args.inp); out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    count = 0
    for p,_,fs in os.walk(inp):
        for f in fs:
            if f.lower().endswith('.parquet'):
                src = pathlib.Path(p)/f
                rel = src.relative_to(inp)
                dst = out/rel.with_suffix('.arrow')
                dst.parent.mkdir(parents=True, exist_ok=True)
                tbl = pq.read_table(src)     # loads file into memory; OK for small demo files
                with ipc.new_file(dst.open('wb'), tbl.schema) as w:
                    w.write_table(tbl)
                count += 1
                print('wrote', dst)
    print('done:', count, 'file(s)')

if __name__ == '__main__':
    main()
