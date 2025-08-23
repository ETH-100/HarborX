#!/usr/bin/env python3
from __future__ import annotations
"""
harborx.add
-----------
Add/merge a remote Lake dataset into local web data folder.

Usage (wired via harborx/cli.py):
  harborx add --base https://play.harborx.tech/data/ --subdir local -m --port 8080
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urljoin, urlparse

# ---------- small utils ----------

def _root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def _say(msg: str) -> None:
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), msg, flush=True)

def _ensure_dir(p: str) -> str:
    os.makedirs(p, exist_ok=True)
    return p

def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _save_json(path: str, obj: dict) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _abs_url(base: str, s: str) -> str:
    """Make s absolute using Lake base; if already absolute, return as-is."""
    if not isinstance(s, str):
        return s
    if s.startswith(("http://", "https://")):
        return s
    while s.startswith("./"):
        s = s[2:]
    return urljoin(base.rstrip("/") + "/", s.lstrip("/"))

def _rewrite_manifest_urls(m: dict, base: str) -> dict:
    """Rewrite arrays in manifest to absolute URLs."""
    out = dict(m)
    out["base"] = base
    for key in ("arrow", "parquet", "files", "urls"):
        if isinstance(out.get(key), list):
            seen, arr = set(), []
            for x in out[key]:
                if isinstance(x, str):
                    ax = _abs_url(base, x)
                    if ax not in seen:
                        seen.add(ax)
                        arr.append(ax)
            out[key] = arr
    if isinstance(out.get("segments"), list):
        segs = []
        for seg in out["segments"]:
            if isinstance(seg, dict) and isinstance(seg.get("file"), str):
                seg = dict(seg)
                seg["file"] = _abs_url(base, seg["file"])
            segs.append(seg)
        out["segments"] = segs
    return out

def _merge_lists(a: list | None, b: list | None) -> list:
    seen, out = set(), []
    for s in (a or []) + (b or []):
        if isinstance(s, str) and s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _merge_segments(a: list | None, b: list | None) -> list:
    idx = {}
    for s in (a or []) + (b or []):
        if not isinstance(s, dict):
            continue
        k = s.get("file") or s.get("path") or s.get("url") or f"anon:{len(idx)}"
        idx[k] = s
    return list(idx.values())

def _merge_manifests(local: dict | None, remote: dict):
    if not local:
        return remote, (0, 0, 0, 0)
    before = (
        len(local.get("arrow") or []),
        len(local.get("parquet") or []),
        len(local.get("files") or []),
        len(local.get("segments") or []),
    )
    merged = {
        "base": remote.get("base") or local.get("base"),
        "arrow": _merge_lists(local.get("arrow"), remote.get("arrow")),
        "parquet": _merge_lists(local.get("parquet"), remote.get("parquet")),
        "files": _merge_lists(local.get("files"), remote.get("files")),
        "urls": _merge_lists(local.get("urls"), remote.get("urls")),
        "segments": _merge_segments(local.get("segments"), remote.get("segments")),
    }
    after = (
        len(merged["arrow"]),
        len(merged["parquet"]),
        len(merged["files"]),
        len(merged["segments"]),
    )
    delta = (after[0] - before[0], after[1] - before[1], after[2] - before[2], after[3] - before[3])
    return merged, delta

def _http_head_or_probe(u: str, timeout=5) -> tuple[bool, int | None]:
    try:
        req = urllib.request.Request(u, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            clen = int(r.headers.get("Content-Length") or 0) or None
            return True, clen
    except Exception:
        try:
            req = urllib.request.Request(u, headers={"Range": "bytes=0-0"})
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

def _download(u: str, dst: Path, overwrite: bool = False):
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    ok, size = _http_head_or_probe(u, timeout=5)
    if dst.exists() and not overwrite and size and dst.stat().st_size == size:
        _say(f"  ↺ skip (exists, size matches): {dst.name}")
        return
    _say(f"  ↓ {u}")
    try:
        with urllib.request.urlopen(u, timeout=30) as r, open(dst, "wb") as f:
            total = int(r.headers.get("Content-Length") or 0)
            read = 0
            chunk = 1 << 20
            t0 = time.time()
            while True:
                buf = r.read(chunk)
                if not buf:
                    break
                f.write(buf)
                read += len(buf)
                # light progress
                if total:
                    pct = int(read * 100 / total)
                    sys.stdout.write(f"\r    {dst.name} {read/1e6:.1f}/{total/1e6:.1f} MB ({pct}%)")
                else:
                    sys.stdout.write(f"\r    {dst.name} {read/1e6:.1f} MB")
                sys.stdout.flush()
            sys.stdout.write(f"\r    {dst.name} done in {time.time()-t0:.1f}s{' '*20}\n")
    except Exception as e:
        _say(f"  ⚠️  download failed: {e}")

def _merge_tables_json(tables_path: Path, new_entries: dict[str, list[str]]) -> dict:
    """Merge new_entries into existing state_diff/_tables.json (append-only, de-dupe)."""
    if tables_path.exists():
        try:
            tables = json.loads(tables_path.read_text(encoding="utf-8"))
        except Exception:
            tables = {}
    else:
        tables = {}
    for k, vs in new_entries.items():
        if not isinstance(vs, list):
            continue
        old = [x for x in tables.get(k, []) if isinstance(x, str)]
        seen = set(old)
        for v in vs:
            if isinstance(v, str) and v not in seen:
                old.append(v)
                seen.add(v)
        tables[k] = old
    tables_path.parent.mkdir(parents=True, exist_ok=True)
    tables_path.write_text(json.dumps(tables, ensure_ascii=False, indent=2), encoding="utf-8")
    return tables

# ---------- main command ----------

def cmd_add(args: argparse.Namespace):
    """
    Fetch manifest from remote Lake, absolutize + incremental-merge into data/<subdir>/.
    Optionally materialize to data/<subdir>/objects/ and rewrite manifest.
    Also merge tables into data/<subdir>/state_diff/_tables.json for tables-first UI.
    Finally, optionally start the static server.
    """
    webroot = os.path.abspath(args.web or os.path.join(_root(), "apps", "web"))
    data_root = _ensure_dir(os.path.join(webroot, args.data))          # apps/web/data
    target = _ensure_dir(os.path.join(data_root, args.subdir))         # apps/web/data/<subdir>
    local_manifest = os.path.join(target, "manifest.json")
    backup_dir = _ensure_dir(os.path.join(target, "_backups"))
    objects_dir = _ensure_dir(os.path.join(target, "objects"))

    # helper: path under target -> "data/<subdir>/..."
    def rel_to_web(p: str | Path) -> str:
        rel = os.path.relpath(os.path.abspath(p), os.path.abspath(webroot)).replace("\\", "/")
        return rel  # already web-root-relative (apps/web/ as doc root)

    base = args.base.rstrip("/")
    tmp = os.path.join(target, "manifest.remote.tmp.json")
    absj = os.path.join(target, "manifest.remote.json")

    _say(f"🔗 Lake base: {base}")
    _say(f"⬇️  Fetching manifest.json …")
    try:
        urllib.request.urlretrieve(base + "/manifest.json", tmp)
    except Exception as e:
        print(f"[ERR] download failed: {e}", file=sys.stderr)
        sys.exit(3)

    _say("🧭 Absolutizing URLs in remote manifest")
    try:
        raw = _load_json(tmp)
    except Exception as e:
        print(f"[ERR] JSON parse failed: {e}", file=sys.stderr)
        sys.exit(4)
    remote = _rewrite_manifest_urls(raw, base)
    _save_json(absj, remote)
    try:
        os.remove(tmp)
    except OSError:
        pass

    # Merge with local manifest
    added = (0, 0, 0, 0)
    if os.path.exists(local_manifest):
        _say("➕ Merging into existing local manifest (append-only)")
        ts = time.strftime("%Y%m%d-%H%M%S")
        _save_json(os.path.join(backup_dir, f"manifest.local.{ts}.json"), _load_json(local_manifest))
        merged, added = _merge_manifests(_load_json(local_manifest), remote)
    else:
        _say("📄 No local manifest found, adopting remote manifest as baseline")
        merged = remote

    # Prepare URLs to optionally materialize
    urls_manifest: list[str] = []
    for k in ("arrow", "parquet", "files"):
        urls_manifest += [u for u in (merged.get(k) or []) if isinstance(u, str)]
    for seg in (merged.get("segments") or []):
        if isinstance(seg, dict) and isinstance(seg.get("file"), str):
            urls_manifest.append(seg["file"])

    # Try to fetch remote _tables.json (for tables-first UI)
    remote_tables: dict[str, list[str]] | None = None
    tables_url = base.rstrip("/") + "/state_diff/_tables.json"
    _say("📋 Fetching remote state_diff/_tables.json (if any)…")
    try:
        with urllib.request.urlopen(tables_url, timeout=5) as r:
            remote_tables = json.loads(r.read().decode("utf-8"))
            _say("  ✓ found")
    except Exception:
        _say("  · not found (will fallback to manifest.parquet → storage_diffs)")

    # If materializing, also add table entries (as absolute urls) to download queue
    urls_tables_abs: list[str] = []
    if remote_tables:
        for arr in remote_tables.values():
            if not isinstance(arr, list):
                continue
            for v in arr:
                if not isinstance(v, str):
                    continue
                if v.startswith(("http://", "https://")):
                    urls_tables_abs.append(v)
                elif v.startswith("data/"):
                    # base is usually ".../data/"; convert "data/fixed/..." -> absolute url
                    urls_tables_abs.append(base.rstrip("/") + "/" + v[len("data/") :])
    # Unique urls
    def _uniq(seq):
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    mapping: dict[str, str] = {}  # absolute-url -> "objects/<file>"
    if args.materialize:
        _say(f"🧱 Materializing → {objects_dir}")
        all_urls = _uniq(urls_manifest + urls_tables_abs)
        for u in all_urls:
            dst = Path(objects_dir) / _stable_name(u)
            _download(u, dst, overwrite=args.overwrite)
            mapping[u] = f"objects/{dst.name}"

        # Rewrite merged manifest to local objects/ paths
        def remap(u: str) -> str:
            return mapping.get(u, u)

        for k in ("arrow", "parquet", "files"):
            if isinstance(merged.get(k), list):
                merged[k] = [remap(u) for u in merged[k] if isinstance(u, str)]

        if isinstance(merged.get("segments"), list):
            new = []
            for seg in merged["segments"]:
                if isinstance(seg, dict) and isinstance(seg.get("file"), str):
                    seg = dict(seg)
                    seg["file"] = remap(seg["file"])
                new.append(seg)
            merged["segments"] = new

    _save_json(local_manifest, merged)

    # Merge tables JSON for tables-first UI
    tables_path = Path(target) / "state_diff" / "_tables.json"  # apps/web/data/<subdir>/state_diff/_tables.json
    new_entries: dict[str, list[str]] = {}

    if remote_tables:
        # Map each entry: absolute URL -> objects/<file> (if materialized) -> "data/<subdir>/objects/<file>"
        for k, arr in remote_tables.items():
            if not isinstance(arr, list):
                continue
            out_list: list[str] = []
            for v in arr:
                if not isinstance(v, str):
                    continue
                # Make absolute url for downloading/mapping
                if v.startswith(("http://", "https://")):
                    abs_u = v
                elif v.startswith("data/"):
                    abs_u = base.rstrip("/") + "/" + v[len("data/") :]
                else:
                    abs_u = _abs_url(base, v)

                if args.materialize and abs_u in mapping:
                    # convert mapping to web-root-relative "data/<subdir>/objects/<file>"
                    out_list.append(rel_to_web(os.path.join(target, mapping[abs_u])))
                else:
                    # keep as absolute URL (frontend accepts absolute too)
                    out_list.append(abs_u)
            if out_list:
                new_entries[k] = out_list
    else:
        # Fallback: treat manifest.parquet as storage_diffs slices
        urls = [u for u in (merged.get("parquet") or []) if isinstance(u, str)]
        if args.materialize:
            out = [rel_to_web(os.path.join(target, mapping.get(u, u))) for u in urls]
        else:
            out = urls
        new_entries = {"storage_diffs": out}

    _ = _merge_tables_json(tables_path, new_entries)
    print(f"[add] merged tables into {rel_to_web(tables_path)}")

    # Probe a few entries
    _say("🔍 Probing a few entries")
    probes = []
    for k in ("arrow", "parquet"):
        if isinstance(merged.get(k), list):
            probes += merged[k][:2]
    if isinstance(merged.get("segments"), list):
        probes += [s.get("file") for s in merged["segments"][:2] if isinstance(s, dict)]
    probes = [p for p in _uniq([p for p in probes if p])]

    ok = fail = 0
    for p in probes:
        if isinstance(p, str) and p.startswith("objects/"):
            if Path(target, p).exists():
                _say(f"  ✅ local OK: {p}")
                ok += 1
            else:
                _say(f"  ⚠️  local missing: {p}")
                fail += 1
        elif isinstance(p, str) and p.startswith(("http://", "https://")):
            good, _ = _http_head_or_probe(p)
            if good:
                _say(f"  ✅ HEAD OK: {p}")
                ok += 1
            else:
                _say(f"  ⚠️  HEAD fail: {p}")
                fail += 1

    # Summary
    _say("📊 Merge summary:")
    print(f"  • arrow:   {len(merged.get('arrow') or [])} items")
    print(f"  • parquet: {len(merged.get('parquet') or [])} items")
    print(f"  • files:   {len(merged.get('files') or [])} items")
    print(f"  • segments:{len(merged.get('segments') or [])} items")
    aA, aP, aF, aS = added
    print(f"  (Δ added) arrow:+{aA} parquet:+{aP} files:+{aF} segments:+{aS}")
    print(f"  Probed OK:{ok} FAIL:{fail}")

    if args.no_serve:
        _say("✅ Done (no-serve).")
        return

    # Start static server
    _say(f"🚀 Starting harborx static server at http://127.0.0.1:{args.port}")
    _say(f"    Serving directory: {webroot}")
    py = sys.executable
    script = os.path.join(_root(), "harborx", "tools", "serve.py")
    if not os.path.exists(script):
        # fallback to /tools/serve.py if repo layout differs
        script = os.path.join(_root(), "tools", "serve.py")
    subprocess.check_call([py, script, webroot, str(args.port)])


# Optional standalone usage: python -m harborx.add --base ...
if __name__ == "__main__":
    ap = argparse.ArgumentParser(prog="harborx-add")
    ap.add_argument("--base", required=True, help="Remote Lake base URL containing manifest.json")
    ap.add_argument("--web", default=os.path.join("apps", "web"), help="Web root directory (default: apps/web)")
    ap.add_argument("--data", default="data", help="Data folder under web root (default: data)")
    ap.add_argument("--subdir", default="local", help="Subdir under data (local|fixed). Default: local")
    ap.add_argument("--port", type=int, default=8080, help="Port for local static server")
    ap.add_argument("--no-serve", action="store_true", help="Do not start server after merging")
    ap.add_argument("--materialize", "-m", action="store_true", help="Download files into objects/ and rewrite manifest/_tables.json")
    ap.add_argument("--overwrite", action="store_true", help="Re-download even if file exists and size matches")
    args = ap.parse_args()
    cmd_add(args)
