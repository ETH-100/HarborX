#!/usr/bin/env python3
import argparse, glob, os, sys, time, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

def one(blob, outdir, chunk):
    out = Path(outdir) / (Path(blob).name.replace(".blob.gz", ".arrow"))
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, os.path.join("scripts","blob_to_arrow.py"),
           "--blob", blob, "--out", str(out), "--chunk", str(chunk)]
    t0 = time.time(); p = subprocess.run(cmd, capture_output=True, text=True); dt = time.time()-t0
    ok = (p.returncode == 0)
    return (blob, str(out), ok, dt, p.stdout, p.stderr)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pattern", default="lake/blob_*.blob.gz")
    ap.add_argument("--outdir", default="lake/hot")
    ap.add_argument("--workers", type=int, default=min(4,(os.cpu_count() or 4)))
    ap.add_argument("--chunk", type=int, default=200_000)
    ap.add_argument("--skip-exists", dest="skip_exists", action="store_true")
    args = ap.parse_args()
    skip_exists = getattr(args, "skip_exists", False)

    blobs = sorted(glob.glob(args.pattern))
    if not blobs:
        print(f"[par] no blobs match {args.pattern}"); return
    if skip_exists:
        def out_for(b): return Path(args.outdir) / (Path(b).name.replace(".blob.gz",".arrow"))
        before = len(blobs)
        blobs = [b for b in blobs if not out_for(b).exists()]
        if before - len(blobs): print(f"[par] skipped {before-len(blobs)} existing file(s)")
        if not blobs: print("[par] nothing to do (all outputs exist)."); return

    print(f"[par] converting {len(blobs)} blobs with {args.workers} worker(s)")
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(one, b, args.outdir, args.chunk) for b in blobs]
        for fut in as_completed(futs):
            blob, out, ok, dt, so, se = fut.result()
            print(f"[par] {'OK' if ok else 'FAIL'} {Path(blob).name} -> {Path(out).name} in {dt:.2f}s")
            if not ok: print("---- stdout ----\n", so, "\n---- stderr ----\n", se)
            results.append((blob,out,ok,dt))
    ok_n = sum(1 for _,_,ok,_ in results if ok)
    print(f"[par] done: {ok_n}/{len(results)} succeeded")
    if ok_n != len(results): sys.exit(1)

if __name__ == "__main__":
    main()
