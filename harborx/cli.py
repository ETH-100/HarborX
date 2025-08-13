#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, sys, json, time, subprocess, urllib.request, urllib.error, hashlib
from urllib.parse import urljoin, urlparse
from pathlib import Path

# --------- utilities (stdlib only) ---------
def _root() -> str:
    return os.path.abspath(os.path.dirname(__file__))

def _say(msg: str):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), msg, flush=True)

def _ensure_dir(p: str) -> str:
    os.makedirs(p, exist_ok=True); return p

def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def _save_json(path: str, obj: dict):
    with open(path, "w", encoding="utf-8") as f: json.dump(obj, f, ensure_ascii=False, indent=2)

def _abs_url(base: str, s: str) -> str:
    if not isinstance(s, str): return s
    if s.startswith(("http://","https://")): return s
    while s.startswith("./"): s = s[2:]
    return urljoin(base.rstrip("/") + "/", s.lstrip("/"))

def _rewrite_manifest_urls(m: dict, base: str) -> dict:
    m = dict(m); m["base"] = base
    for key in ("arrow","parquet","files","urls"):
        if isinstance(m.get(key), list):
            seen=set(); out=[]
            for x in m[key]:
                if isinstance(x,str):
                    ax = _abs_url(base,x)
                    if ax not in seen: seen.add(ax); out.append(ax)
            m[key] = out
    if isinstance(m.get("segments"), list):
        new=[]
        for seg in m["segments"]:
            if isinstance(seg,dict) and isinstance(seg.get("file"),str):
                seg = dict(seg); seg["file"] = _abs_url(base, seg["file"])
            new.append(seg)
        m["segments"] = new
    return m

def _merge_manifests(local: dict|None, remote: dict):
    if not local: return remote,(0,0,0,0)
    def uniq_merge(a,b):
        seen=set(); out=[]
        for s in (a or [])+(b or []):
            if isinstance(s,str) and s not in seen: seen.add(s); out.append(s)
        return out
    def merge_segments(a,b):
        idx={}
        for s in (a or [])+(b or []):
            if not isinstance(s,dict): continue
            k = s.get("file") or s.get("path") or s.get("url") or f"anon:{len(idx)}"
            idx[k]=s
        return list(idx.values())
    before = (len(local.get("arrow") or []),
              len(local.get("parquet") or []),
              len(local.get("files") or []),
              len(local.get("segments") or []))
    merged = {
        "base": remote.get("base") or local.get("base"),
        "arrow":   uniq_merge(local.get("arrow"),   remote.get("arrow")),
        "parquet": uniq_merge(local.get("parquet"), remote.get("parquet")),
        "files":   uniq_merge(local.get("files"),   remote.get("files")),
        "urls":    uniq_merge(local.get("urls"),    remote.get("urls")),
        "segments":merge_segments(local.get("segments"), remote.get("segments")),
    }
    after = (len(merged["arrow"]), len(merged["parquet"]), len(merged["files"]), len(merged["segments"]))
    return merged, (after[0]-before[0], after[1]-before[1], after[2]-before[2], after[3]-before[3])

def _http_head_or_probe(u: str, timeout=5) -> tuple[bool,int|None]:
    try:
        req = urllib.request.Request(u, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            clen = int(r.headers.get("Content-Length") or 0) or None
            return True, clen
    except Exception:
        try:
            req = urllib.request.Request(u, headers={"Range":"bytes=0-0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                clen = int(r.headers.get("Content-Length") or 0) or None
                return True, clen
        except Exception:
            return False, None

def _pick_ext(u: str, default=".bin"):
    p = urlparse(u).path
    ext = os.path.splitext(p)[1].lower()
    return ext or default

def _stable_name(u: str) -> str:
    h = hashlib.blake2b(u.encode("utf-8"), digest_size=8).hexdigest()
    return f"{h}{_pick_ext(u)}"

def _download(u: str, dst: Path, overwrite: bool=False):
    dst.parent.mkdir(parents=True, exist_ok=True)
    ok, size = _http_head_or_probe(u, timeout=5)
    if dst.exists() and not overwrite and size and dst.stat().st_size == size:
        _say(f"  ↺ skip (exists, size matches): {dst.name}")
        return
    _say(f"  ↓ {u}")
    try:
        with urllib.request.urlopen(u, timeout=30) as r, open(dst, "wb") as f:
            total = int(r.headers.get("Content-Length") or 0)
            read = 0; chunk = 1<<20; t0=time.time()
            while True:
                buf = r.read(chunk)
                if not buf: break
                f.write(buf); read += len(buf)
                if total:
                    pct = int(read*100/total)
                    sys.stdout.write(f"\r    {dst.name} {read/1e6:.1f}/{total/1e6:.1f} MB ({pct}%)")
                else:
                    sys.stdout.write(f"\r    {dst.name} {read/1e6:.1f} MB")
                sys.stdout.flush()
            sys.stdout.write(f"\r    {dst.name} done in {time.time()-t0:.1f}s{' '*20}\n")
    except Exception as e:
        _say(f"  ⚠️  download failed: {e}")

# --------- subcommands ---------
def cmd_blobscan(args):
    try:
        from harborx.blobscan import run_blobscan_fetch
    except Exception:
        print("Missing extras. Please install: pip install -e .[cli]", file=sys.stderr)
        raise
    os.makedirs(args.out, exist_ok=True)
    run_blobscan_fetch(
        base=args.base, limit=args.limit, offset=args.offset, out_dir=args.out,
        chain_id=args.chain_id, write_parquet=args.parquet,
        core_addr=(args.core or "").lower(), debug=args.debug,
    )

def cmd_manifest(args):
    from tools import build_manifest
    build_manifest(root=args.root, data=args.data, include_parquet=args.include_parquet)

def cmd_serve(args):
    webroot = os.path.abspath(args.dir or os.path.join(_root(), "apps", "web"))
    py = sys.executable
    script = os.path.join(_root(), "tools", "serve.py")
    subprocess.check_call([py, script, webroot, str(args.port)])

def cmd_add(args):
    """
    Fetch manifest from remote Lake, absolutize + incremental-merge into data/<subdir>/,
    optional materialize(download) to data/<subdir>/objects/, then serve.
    """
    webroot   = os.path.abspath(args.web or os.path.join(_root(), "apps", "web"))
    data_root = _ensure_dir(os.path.join(webroot, args.data))
    target    = _ensure_dir(os.path.join(data_root, args.subdir))
    local_manifest = os.path.join(target, "manifest.json")
    backup_dir     = _ensure_dir(os.path.join(target, "_backups"))
    objects_dir    = _ensure_dir(os.path.join(target, "objects"))

    base = args.base.rstrip("/")
    tmp  = os.path.join(target, "manifest.remote.tmp.json")
    absj = os.path.join(target, "manifest.remote.json")

    _say(f"🔗 Using Lake base: {base}")
    _say(f"⬇️  Fetching manifest.json from {base}")
    try:
        urllib.request.urlretrieve(base + "/manifest.json", tmp)
    except Exception as e:
        print(f"[ERR] download failed: {e}", file=sys.stderr); sys.exit(3)

    _say("🧭 Rewriting relative paths → absolute URLs")
    try:
        raw = _load_json(tmp)
    except Exception as e:
        print(f"[ERR] JSON parse failed: {e}", file=sys.stderr); sys.exit(4)
    remote = _rewrite_manifest_urls(raw, base)
    _save_json(absj, remote); 
    try: os.remove(tmp)
    except OSError: pass

    added=(0,0,0,0)
    if os.path.exists(local_manifest):
        _say("➕ Merging remote manifest into existing local manifest (incremental union)")
        ts = time.strftime("%Y%m%d-%H%M%S")
        _save_json(os.path.join(backup_dir, f"manifest.local.{ts}.json"), _load_json(local_manifest))
        merged, added = _merge_manifests(_load_json(local_manifest), remote)
    else:
        _say("📄 No local manifest found, adopting remote manifest as local")
        merged = remote

    if args.materialize:
        _say(f"🧱 Materializing → {objects_dir}")
        urls=[]
        for k in ("arrow","parquet","files"):
            urls += [u for u in (merged.get(k) or []) if isinstance(u,str)]
        for seg in (merged.get("segments") or []):
            if isinstance(seg,dict) and isinstance(seg.get("file"),str): urls.append(seg["file"])
        # unique keep order
        seen=set(); uniq=[]
        for u in urls:
            if u not in seen: seen.add(u); uniq.append(u)
        # download
        mapping={}
        for u in uniq:
            dst = Path(objects_dir) / _stable_name(u)
            _download(u, dst, overwrite=args.overwrite)
            mapping[u] = f"objects/{dst.name}"
        # rewrite manifest to local paths (relative to manifest)
        def remap(u): return mapping.get(u,u)
        for k in ("arrow","parquet","files"):
            if isinstance(merged.get(k), list):
                merged[k] = [remap(u) for u in merged[k] if isinstance(u,str)]
        if isinstance(merged.get("segments"), list):
            new=[]
            for seg in merged["segments"]:
                if isinstance(seg,dict) and isinstance(seg.get("file"),str):
                    seg = dict(seg); seg["file"] = remap(seg["file"])
                new.append(seg)
            merged["segments"]=new

    _save_json(local_manifest, merged)

    _say("🔍 Validating a few entries")
    probes=[]
    for k in ("arrow","parquet"):
        if isinstance(merged.get(k),list): probes += merged[k][:2]
    if isinstance(merged.get("segments"),list):
        probes += [s.get("file") for s in merged["segments"][:2] if isinstance(s,dict)]
    probes = [p for p in dict.fromkeys([p for p in probes if p])]

    ok=fail=0
    for p in probes:
        if p.startswith("objects/"):
            if Path(target,p).exists(): _say(f"  ✅ local OK: {p}"); ok+=1
            else: _say(f"  ⚠️  local missing: {p}"); fail+=1
        else:
            good,_ = _http_head_or_probe(p)
            if good: _say(f"  ✅ HEAD OK: {p}"); ok+=1
            else:    _say(f"  ⚠️  HEAD fail: {p}"); fail+=1

    _say("📊 Merge summary:")
    print(f"  • arrow:   {len(merged.get('arrow') or [])} items")
    print(f"  • parquet: {len(merged.get('parquet') or [])} items")
    print(f"  • files:   {len(merged.get('files') or [])} items")
    print(f"  • segments:{len(merged.get('segments') or [])} items")
    aA,aP,aF,aS = added
    print(f"  (Δ added) arrow:+{aA} parquet:+{aP} files:+{aF} segments:+{aS}")
    print(f"  Probed OK:{ok} FAIL:{fail}")

    if args.no_serve:
        _say("✅ Done (no-serve)."); return
    _say(f"🚀 Starting harborx static server at http://127.0.0.1:{args.port}")
    _say(f"    Serving directory: {webroot} (manifest at {local_manifest})")
    class _S: pass
    s=_S(); s.dir=webroot; s.port=args.port
    cmd_serve(s)

# --------- argparse wiring ---------
def main():
    ap = argparse.ArgumentParser(prog="harborx", description="HarborX unified CLI")
    sp = ap.add_subparsers(dest="cmd", required=True)

    p = sp.add_parser("blobscan", help="Fetch latest blobs -> Arrow/Parquet + manifest")
    p.add_argument("--base", default="https://api.blobscan.com")
    p.add_argument("--limit", type=int, default=3)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--out", default=os.path.join("apps","web","data"))
    p.add_argument("--chain-id", type=int, default=1)
    p.add_argument("--parquet", action="store_true")
    p.add_argument("--core")
    p.add_argument("--debug", action="store_true")
    p.set_defaults(func=cmd_blobscan)

    m = sp.add_parser("manifest", help="Rebuild manifest.json for the web demo")
    m.add_argument("--root", default=os.path.join("apps","web"))
    m.add_argument("--data", default="data")
    m.add_argument("--include-parquet", action="store_true")
    m.set_defaults(func=cmd_manifest)

    w = sp.add_parser("serve", help="Serve the web demo (static server)")
    w.add_argument("--dir", default=os.path.join("apps","web"))
    w.add_argument("--port", type=int, default=8080)
    w.set_defaults(func=cmd_serve)

    r = sp.add_parser("add", help="Pull remote Lake manifest, merge into data/<subdir>/; optional download; then serve")
    r.add_argument("--base", required=True, help="Remote Lake base URL containing manifest.json")
    r.add_argument("--web",  default=os.path.join("apps","web"))
    r.add_argument("--data", default="data")
    r.add_argument("--subdir", default="local", help="Write under data/<subdir>/ (use 'fixed' for online assets)")
    r.add_argument("--port", type=int, default=8080)
    r.add_argument("--no-serve", action="store_true")
    r.add_argument("--materialize", "-m", action="store_true", help="Download files into objects/ and rewrite manifest")
    r.add_argument("--overwrite", action="store_true", help="Re-download even if file size matches")
    r.set_defaults(func=cmd_add)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
