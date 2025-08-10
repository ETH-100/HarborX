#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, sys, subprocess

def _root():
    return os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

def cmd_blobscan(args):
    """Fetch blobs via Blobscan API and write Arrow/Parquet + manifest."""
    try:
        from harborx.blobscan import run_blobscan_fetch
    except Exception:
        print("Missing extras. Please install: pip install -e .[cli]", file=sys.stderr)
        raise
    os.makedirs(args.out, exist_ok=True)
    run_blobscan_fetch(
        base=args.base,
        limit=args.limit,
        offset=args.offset,
        out_dir=args.out,
        chain_id=args.chain_id,
        write_parquet=args.parquet,
        core_addr=(args.core or "").lower(),
        debug=args.debug,
    )

def cmd_rollup(args):
    """(Optional) EL/Beacon path (disabled for PoC)."""
    try:
        from harborx.rollup import run_rollup_poc
    except Exception:
        print("Missing extras. Please install: pip install -e .[cli]", file=sys.stderr)
        raise
    os.makedirs(args.out, exist_ok=True)
    run_rollup_poc(
        el_rpc=args.el_rpc,
        beacon=args.beacon,
        core=args.core,
        blocks=args.blocks,
        slots=args.slot_range,
        out_dir=args.out,
        chain_id=args.chain_id,
        write_parquet=args.parquet,
        limit=args.limit_blobs,
        qps=args.qps,
        retries=args.retries,
    )

def cmd_manifest(args):
    """Build manifest.json for the web demo."""
    from harborx.tools import build_manifest
    build_manifest(root=args.root, data=args.data, include_parquet=args.include_parquet)

def cmd_serve(args):
    """Serve static folder with correct MIME types."""
    webroot = os.path.abspath(args.dir or os.path.join(_root(), "apps", "web"))
    py = sys.executable
    script = os.path.join(os.path.dirname(__file__), "tools", "serve.py")
    subprocess.check_call([py, script, webroot, str(args.port)])

def cmd_tidy(args):
    """Dry-run repo re-org; use --apply to move."""
    from harborx.tools import tidy_repo
    tidy_repo(apply=args.apply, aggressive=args.aggressive)

def main():
    ap = argparse.ArgumentParser(prog="harborx", description="HarborX unified CLI")
    sp = ap.add_subparsers(dest="cmd", required=True)

    p = sp.add_parser("blobscan", help="Fetch latest blobs from Blobscan API -> Arrow/Parquet + manifest")
    p.add_argument("--base", default="https://api.blobscan.com")
    p.add_argument("--limit", type=int, default=3)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--out", default="apps/web/data")
    p.add_argument("--chain-id", type=int, default=1)
    p.add_argument("--parquet", action="store_true")
    p.add_argument("--core")
    p.add_argument("--debug", action="store_true")
    p.set_defaults(func=cmd_blobscan)

    s = sp.add_parser("rollup", help="(Optional) EL/Beacon path (rate-limited)")
    s.add_argument("--el-rpc", required=True)
    s.add_argument("--beacon", required=True)
    s.add_argument("--blocks", type=int, default=500)
    s.add_argument("--slot-range", type=int, default=1800)
    s.add_argument("--core", default="0xc662c410C0ECf747543f5bA90660f6ABeBD9C8c4")
    s.add_argument("--out", default="apps/web/data")
    s.add_argument("--chain-id", type=int, default=1)
    s.add_argument("--parquet", action="store_true")
    s.add_argument("--limit-blobs", type=int, default=3)
    s.add_argument("--qps", type=float, default=2.0)
    s.add_argument("--retries", type=int, default=4)
    s.set_defaults(func=cmd_rollup)

    m = sp.add_parser("manifest", help="Rebuild manifest.json for the web demo")
    m.add_argument("--root", default="apps/web")
    m.add_argument("--data", default="data")
    m.add_argument("--include-parquet", action="store_true")
    m.set_defaults(func=cmd_manifest)

    w = sp.add_parser("serve", help="Serve the web demo (static server)")
    w.add_argument("--dir", default="apps/web")
    w.add_argument("--port", type=int, default=8080)
    w.set_defaults(func=cmd_serve)

    t = sp.add_parser("tidy", help="Print (or apply) repo re-organization plan")
    t.add_argument("--apply", action="store_true")
    t.add_argument("--aggressive", action="store_true", help="Also move legacy scripts into bench/")
    t.set_defaults(func=cmd_tidy)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
