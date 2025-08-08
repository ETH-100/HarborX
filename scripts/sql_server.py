#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FastAPI server for running ad-hoc SQL against:
- DuckDB over Arrow files in ./lake/hot  (two modes: arrow_bridge, arrow_ext)
- SQLite database at ./lake/sqlite.db

Static frontend is served from ./web at /ui (root / redirects to /ui/).

Run:
  uvicorn scripts.sql_server:app --reload --port 8000
Then open http://localhost:8000/
"""

import os
import time
import threading
from pathlib import Path
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Paths
ARROW_DIR = Path("demo/lake/hot")
SQLITE_DB = Path("demo/lake/sqlite.db")
WEB_DIR = Path("web")  # contains index.html

app = FastAPI(title="HarborX SQL Demo", version="0.1.0")

# CORS (open for demo)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# -------------------- Models --------------------

class QueryBody(BaseModel):
    engine: str  # "arrow_bridge" | "arrow_ext" | "sqlite"
    sql: str


# -------------------- DuckDB (Arrow) --------------------

_arrow_lock = threading.Lock()
_duckdb_con = None  # type: ignore
_arrow_mode = None  # "arrow_bridge" | "arrow_ext"

def _init_duckdb(engine: str):
    """Initialize a DuckDB connection and create 'state' view over Arrow files."""
    global _duckdb_con, _arrow_mode
    import duckdb
    import pyarrow.ipc as ipc

    if not ARROW_DIR.exists():
        raise HTTPException(status_code=400, detail=f"Arrow dir not found: {ARROW_DIR}")
    paths = sorted(str(p).replace("\\", "/") for p in ARROW_DIR.glob("*.arrow"))
    if not paths:
        raise HTTPException(status_code=400, detail=f"No .arrow files in {ARROW_DIR}")

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={os.cpu_count() or 4};")

    if engine == "arrow_ext":
        # Use DuckDB's native Arrow scanner (read_ipc). On newer builds Arrow may be built-in.
        try:
            con.execute("INSTALL 'arrow'; LOAD 'arrow';")
        except Exception:
            pass
        union_sql = " UNION ALL ".join([f"SELECT * FROM read_ipc('{p}')" for p in paths])
        con.execute("CREATE OR REPLACE VIEW state AS " + union_sql)
    else:
        # Default: pyarrow bridge — register Arrow tables as in-memory relations
        views = []
        for i, p in enumerate(paths):
            tbl = ipc.open_file(p).read_all()
            name = f"t{i}"
            con.register(name, tbl)
            views.append(f"SELECT * FROM {name}")
        con.execute("CREATE OR REPLACE VIEW state AS " + " UNION ALL ".join(views))

    _duckdb_con = con
    _arrow_mode = engine


def _duckdb_query(sql: str, engine: str) -> Dict[str, Any]:
    import pyarrow as pa  # noqa: F401  (ensures arrow types available to duckdb)
    with _arrow_lock:
        if _duckdb_con is None or _arrow_mode != engine:
            _init_duckdb(engine)

        t0 = time.perf_counter()
        table = _duckdb_con.execute(sql).fetch_arrow_table()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

    def cell(x):
        # Convert binary-like payloads to hex for JSON
        if isinstance(x, (bytes, bytearray, memoryview)):
            return "0x" + bytes(x).hex()
        return x

    cols = [f.name for f in table.schema]
    rows: List[List[Any]] = []
    for batch in table.to_batches():
        cols_arrays = [col.to_pylist() for col in batch.columns]
        for i in range(batch.num_rows):
            rows.append([cell(cols_arrays[j][i]) for j in range(len(cols_arrays))])

    return {
        "columns": cols,
        "rows": rows,
        "row_count": table.num_rows,
        "elapsed_ms": round(elapsed_ms, 3),
    }


# -------------------- SQLite --------------------

def _sqlite_query(sql: str) -> Dict[str, Any]:
    import sqlite3
    if not SQLITE_DB.exists():
        raise HTTPException(status_code=400, detail=f"SQLite DB not found: {SQLITE_DB}")

    con = sqlite3.connect(str(SQLITE_DB))
    try:
        t0 = time.perf_counter()
        cur = con.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        cols = [d[0] for d in cur.description] if cur.description else []

        def cell(x):
            if isinstance(x, (bytes, bytearray, memoryview)):
                return "0x" + bytes(x).hex()
            return x

        out_rows = [[cell(v) for v in r] for r in rows]
        return {
            "columns": cols,
            "rows": out_rows,
            "row_count": len(out_rows),
            "elapsed_ms": round(elapsed_ms, 3),
        }
    finally:
        con.close()


# -------------------- API --------------------

@app.get("/api/health")
def health():
    return {"ok": True}

@app.post("/api/query")
def run_query(body: QueryBody):
    sql = (body.sql or "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="Empty SQL")
    engine = (body.engine or "arrow_bridge").lower()
    if engine not in ("arrow_bridge", "arrow_ext", "sqlite"):
        raise HTTPException(status_code=400, detail="engine must be one of: arrow_bridge, arrow_ext, sqlite")
    try:
        if engine.startswith("arrow"):
            return _duckdb_query(sql, engine)
        else:
            return _sqlite_query(sql)
    except HTTPException:
        raise
    except Exception as e:
        # Return a friendly message instead of a 500
        raise HTTPException(status_code=400, detail=f"Query failed: {e}")


# -------------------- Static Frontend --------------------
# Mount the simple HTML UI at /ui, and redirect / -> /ui/
if WEB_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(WEB_DIR), html=True), name="web")

@app.get("/", include_in_schema=False)
def _root():
    # Always redirect to /ui/ (trailing slash so index.html is auto-resolved)
    return RedirectResponse("/ui/")
