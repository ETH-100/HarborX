#!/usr/bin/env python3
from __future__ import annotations
import os, sys, argparse
from harborx.add import cmd_add

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from blobscan import sn_pipeline

def cmd_sn(args: argparse.Namespace) -> None:
    sn_pipeline(
        out_dir=args.out,
        chain_id=args.chain_id,
        page=args.page,
        page_size=args.page_size,
        max_items=args.max,
        start_block=args.start_block,
        end_block=args.end_block,
        storage_only=args.storage_only,
        download_only=args.download_only,
        decoder_path=args.decoder,
        decoder_config=args.decoder_config,
        decoder_cache=args.decoder_cache,
        flip_endian=args.flip_endian,
        debug=args.debug,
    )

def main() -> None:
    ap = argparse.ArgumentParser(prog="harborx-cli")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("sn", help="Download Starknet blobs and decode via tools/starknet-scrape using a manifest.")
    sp.add_argument("--out", required=True, help="Output base dir (raw_bin/ and manifest live under this).")
    sp.add_argument("--chain-id", type=int, default=1)
    sp.add_argument("--page", type=int, default=1)
    sp.add_argument("--page-size", type=int, default=50)
    sp.add_argument("--max", type=int, default=20, help="Download at most N blobs from this page.")
    sp.add_argument("--start-block", type=int, default=0, help="Filter by L1 start block (inclusive).")
    sp.add_argument("--end-block", type=int, default=0, help="Filter by L1 end block (inclusive).")
    sp.add_argument("--storage-only", action="store_true", help="Prefer storage url fields only.")
    sp.add_argument("--download-only", action="store_true", help="Skip decoding (download .bin only).")
    sp.add_argument("--flip-endian", action="store_true", help="(reserved) flip byte order in 32B words.")
    sp.add_argument("--decoder", default="", help="Path to tools/starknet-scrape(.exe). Default: auto under ./tools/")
    sp.add_argument("--decoder-config", default="", help="Path to decoder config TOML. Default: ./decoder.toml")
    sp.add_argument("--decoder-cache", default="", help="Decoder work/output dir. Default: ./decoder_cache")
    sp.add_argument("--debug", action="store_true")
    sp.set_defaults(func=cmd_sn)

    sp_add = sub.add_parser("add", help="Pull remote Lake manifest, merge into data/<subdir>/; optional download; then serve")
    sp_add.add_argument("--base", required=True, help="Remote Lake base URL containing manifest.json")
    sp_add.add_argument("--web",  default=os.path.join("apps","web"))
    sp_add.add_argument("--data", default="data")
    sp_add.add_argument("--subdir", default="local", help="Write under data/<subdir>/ (use 'fixed' for online assets)")
    sp_add.add_argument("--port", type=int, default=8080)
    sp_add.add_argument("--no-serve", action="store_true")
    sp_add.add_argument("--materialize", "-m", action="store_true", help="Download files into objects/ and rewrite manifest")
    sp_add.add_argument("--overwrite", action="store_true", help="Re-download even if file size matches")
    sp_add.set_defaults(func=cmd_add)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
