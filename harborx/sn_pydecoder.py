# -*- coding: utf-8 -*-
# sn_pydecoder.py — robust Starknet blob → KV decoder
from __future__ import annotations

from typing import List, Dict, Tuple, Iterable, Optional

import os, json, sqlite3

# ----------------------------- Imports (vendored) -----------------------------

# 1) DA unpack: Fr coeffs -> Cairo felts
try:
    from harborx.vendor.da_unp import fr_coeffs_to_cairo_felts  # type: ignore
except Exception as _e:
    fr_coeffs_to_cairo_felts = None  # type: ignore

# 2) Stateless decompress: prefer official vendored version, else minimal clone
try:
    from harborx.vendor.stateless_official import decompress as _official_decompress  # type: ignore
except Exception:
    _official_decompress = None

try:
    from harborx.vendor.stateless_minimal import decompress as _minimal_decompress  # type: ignore
except Exception:
    _minimal_decompress = None

def _get_stateless_decompress():
    if _official_decompress is not None:
        return _official_decompress
    if _minimal_decompress is not None:
        return _minimal_decompress
    raise ImportError("No stateless decompressor available. Ensure vendored files exist.")

# 3) Program output parser (uncompressed felts -> locate and parse state diff)
from harborx.vendor.program_output_minimal import extract_state_diff  # type: ignore

# ----------------------------- Cairo field (Fr) -------------------------------

# Cairo felt field modulus: 2^251 + 17*2^192 + 1
FR_MOD = (1 << 251) + 17 * (1 << 192) + 1
GENERATOR = 5  # primitive root used by Starknet DA for length-4096 NTT

# ----------------------------- Utilities -------------------------------------

def _u32chunks(b: bytes, n: int) -> Iterable[bytes]:
    for i in range(0, len(b), n):
        yield b[i:i+n]

def _blob_to_evals_32B_words(blob: bytes, *, endian: str = "BE") -> List[int]:
    """
    Interpret the 131072-byte blob as 4096 field elements (32-byte words).
    Values are taken modulo FR_MOD to be safe.
    """
    if len(blob) != 131072:
        raise ValueError(f"blob length must be 131072 bytes, got {len(blob)}")
    out: List[int] = []
    for w in _u32chunks(blob, 32):
        v = int.from_bytes(w, byteorder="big" if endian=="BE" else "little", signed=False)
        out.append(v % FR_MOD)
    if len(out) != 4096:
        raise AssertionError("expected 4096 evals per blob")
    return out

def _bit_reverse(x: int, bits: int) -> int:
    y = 0
    for _ in range(bits):
        y = (y << 1) | (x & 1)
        x >>= 1
    return y

def _bit_reverse_list(a: List[int]) -> None:
    n = len(a)
    bits = (n - 1).bit_length()
    for i in range(n):
        j = _bit_reverse(i, bits)
        if j > i:
            a[i], a[j] = a[j], a[i]

def _egcd(a: int, b: int) -> Tuple[int,int,int]:
    if b == 0:
        return (a, 1, 0)
    g, x1, y1 = _egcd(b, a % b)
    return (g, y1, x1 - (a // b) * y1)

def _modinv(a: int, mod: int = FR_MOD) -> int:
    a %= mod
    g, x, _ = _egcd(a, mod)
    if g != 1:
        raise ZeroDivisionError("no modular inverse")
    return x % mod

def _root_of_unity(n: int) -> int:
    # Since 4096 | (FR_MOD - 1), generator^((FR_MOD-1)/n) is an n-th root.
    return pow(GENERATOR, (FR_MOD - 1) // n, FR_MOD)

def _ntt_inplace(a: List[int], inverse: bool = False, mod: int = FR_MOD) -> None:
    n = len(a)
    _bit_reverse_list(a)
    m = 2
    while m <= n:
        step = pow(_root_of_unity(n), n // m, mod)
        if inverse:
            step = _modinv(step, mod)
        for k in range(0, n, m):
            w = 1
            half = m // 2
            for j in range(half):
                u = a[k + j]
                t = (a[k + j + half] * w) % mod
                a[k + j] = (u + t) % mod
                a[k + j + half] = (u - t) % mod
                w = (w * step) % mod
        m <<= 1
    if inverse:
        inv_n = _modinv(n, mod)
        for i in range(n):
            a[i] = (a[i] * inv_n) % mod

def _ifft_one_blob(evals: List[int], *, bitrev: bool = True) -> List[int]:
    """
    In-place inverse NTT to obtain coefficients from evaluation values.
    Starknet blobs require bit-reversal before inverse; we mimic common pipelines.
    """
    arr = list(evals)
    # Typical Starknet DA expects bit-reversed order for inverse transform.
    if bitrev:
        _bit_reverse_list(arr)
    _ntt_inplace(arr, inverse=True, mod=FR_MOD)
    return arr

# -------------------------- Stateless header scanning -------------------------

def _try_stateless_decompress_scan(felts: List[int], *, max_scan: int = 16384, debug: bool = False) -> Tuple[List[int], int]:
    """
    Try to find the start of the stateless-compressed stream by scanning offsets.
    Returns (uncompressed_program_output_felts, header_offset).
    """
    decompress = _get_stateless_decompress()
    last_err: Optional[str] = None
    limit = min(max_scan, len(felts))
    for off in range(limit):
        try:
            uncompressed = list(decompress(felts[off:]))
            if not uncompressed:
                raise ValueError("empty uncompressed stream")
            if debug:
                print(f"[stateless] candidate header@{off}, uncompressed_len={len(uncompressed)}")
            return uncompressed, off
        except Exception as e:
            last_err = str(e)[:120]
            continue
    raise EOFError(f"bitstream exhausted (scanned 0..{limit-1}); last error: {last_err}")

# -------------------------- v0.13.x index map (optional) ----------------------

class StatefulIndexMap:
    """
    Optional resolver for v0.13.x stateful encoding where addresses/keys may be
    replaced by indices. Expects a sqlite db file with table:
      CREATE TABLE IF NOT EXISTS idxmap (idx INTEGER PRIMARY KEY, val TEXT);
    where val is decimal string of the 251-bit felt value.
    """
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._ok = os.path.exists(db_file)

    def get(self, idx: int) -> Optional[int]:
        if not self._ok:
            return None
        try:
            with sqlite3.connect(self.db_file) as cx:
                row = cx.execute("SELECT val FROM idxmap WHERE idx=?", (idx,)).fetchone()
            if not row:
                return None
            v = int(row[0])
            return v
        except Exception:
            return None

    def maybe_addr(self, addr_or_idx: int) -> int:
        # Heuristic: small values are literal; big (>2^127) likely an index surrogate
        if addr_or_idx.bit_length() <= 127:
            return addr_or_idx
        v = self.get(addr_or_idx)
        return v if v is not None else addr_or_idx

    def maybe_key(self, key_or_idx: int) -> int:
        if key_or_idx.bit_length() <= 127:
            return key_or_idx
        v = self.get(key_or_idx)
        return v if v is not None else key_or_idx

# -------------------------- Flatten state-diff to KV --------------------------

def _flatten_parsed(parsed: Dict, resolver: StatefulIndexMap) -> List[Dict]:
    """
    parsed: {"contracts":[{"address":int,"class_hash":opt int,"nonce":opt int,"storage":[[k,v],...] }], "declared":[[class_hash,version], ...]}
    """
    out: List[Dict] = []
    contracts = parsed.get("contracts", [])
    for c in contracts:
        addr = resolver.maybe_addr(int(c.get("address", 0)))
        ch = c.get("class_hash")
        nn = c.get("nonce")
        for pair in c.get("storage", []):
            if isinstance(pair, dict):
                k = pair.get("key"); v = pair.get("value")
            else:
                # assume [k,v]
                k, v = pair
            k = resolver.maybe_key(int(k))
            v = int(v)
            out.append({
                "addr": str(addr),
                "key": str(k),
                "value": str(v),
                **({"class_hash": str(int(ch))} if ch is not None else {}),
                **({"nonce": str(int(nn))} if nn is not None else {}),
            })
    return out

# -------------------- Top-level: decode blobs -> KV rows --------------------

def decode_blob_bins_to_kv(
    blobs: List[bytes],
    stateful_db_file: str,
    *,
    debug: bool = False,
    force_ifft: bool = False,  # placeholder
) -> List[Dict]:
    """
    Input: several blob binaries (same L1 tx/frame), index 0..k.
    Output: flattened (addr,key,value,...) rows extracted from state-diff.
    """
    if fr_coeffs_to_cairo_felts is None:
        raise ImportError("DA unpacker not available. Please vendor Starknet DA packing/unpacking code (see harborx/vendor/da_unp.py).")

    # 1) evals -> coeffs (IFFT) per blob
    coeffs_per_blob: List[List[int]] = []
    for i, b in enumerate(blobs):
        evals = _blob_to_evals_32B_words(b, endian="BE")
        coeffs = _ifft_one_blob(evals, bitrev=True)
        coeffs_per_blob.append(coeffs)
        if debug:
            print(f"[blob#{i}] evals={len(evals)} -> coeffs={len(coeffs)} (concat_total={4096*len(coeffs_per_blob)})")

    # 2) DA unpack: coeffs_per_blob -> Cairo felts stream
    felts: List[int] = list(fr_coeffs_to_cairo_felts(coeffs_per_blob))
    if debug:
        print(f"[da] coeffs_per_blob={len(coeffs_per_blob)} -> felts={len(felts)}")

    # 3) Scan & stateless decompress to get uncompressed program output felts
    uncompressed, header_off = _try_stateless_decompress_scan(felts, max_scan=16384, debug=debug)
    if debug:
        print(f"[stateless] header@{header_off}, uncompressed_len={len(uncompressed)}")

    # 4) Locate and parse state-diff from uncompressed output
    #    extract_state_diff may return (parsed, start, used)
    parsed: Dict
    try:
        parsed, start, used = extract_state_diff(uncompressed, debug=debug)  # type: ignore
        if debug:
            print(f"[po] state-diff@{start} used={used}")
    except TypeError:
        # older signature returning dict only
        parsed = extract_state_diff(uncompressed, debug=debug)  # type: ignore

    # 5) Flatten
    resolver = StatefulIndexMap(stateful_db_file)
    rows = _flatten_parsed(parsed, resolver)
    return rows

# -------------------- Helper: files -> jsonl --------------------

def decode_bin_files_to_json(
    bin_paths: List[str],
    stateful_db_file: str,
    out_jsonl: str,
    *,
    debug: bool = False,
    force_ifft: bool = False,
) -> str:
    blobs = [open(p, "rb").read() for p in bin_paths]
    rows = decode_blob_bins_to_kv(
        blobs,
        stateful_db_file,
        debug=debug,
        force_ifft=force_ifft,
    )
    os.makedirs(os.path.dirname(out_jsonl), exist_ok=True)
    with open(out_jsonl, "w", encoding="utf-8") as fp:
        for r in rows:
            fp.write(json.dumps(r) + "\n")
    return os.path.abspath(out_jsonl)
