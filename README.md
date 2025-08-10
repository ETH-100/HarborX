# HarborX — Blob Direct Write & High-Performance SQL

HarborX is a high-performance data engine for Web3. It ingests incremental **blob** data from L2/rollup ecosystems, writes directly into **columnar formats** (Arrow/Parquet), and exposes a **standard SQL** interface that runs either fully in the browser (DuckDB-WASM) or on your backend. The long-term goal is a verifiable pipeline with **ZK proofs** (ZKSQL) for trusted computation and cross-chain verification.

## Key Features

- **Blob → Columnar (Direct Write)**
    
    Pull real blob payloads and write straight to Arrow/Parquet—no heavyweight node sync or custom ETL needed. Supports small, incremental updates for low latency.
    
- **SQL Anywhere (Frontend-Only or Backend)**
    
    Query latest and historical data with SQL. The PoC ships a **pure-frontend** demo (DuckDB-WASM) that reads Arrow/Parquet over HTTP, no server code required.
    
- **Fast & Lightweight**
    
    Columnar storage + on-demand loading means less I/O and memory. PoC keeps things simple; you can scale up later with distributed engines (e.g., DataFusion/others).
    
- **ZKSQL (Experimental Roadmap)**
    
    Design path toward verifiable writes and query proofs, enabling trust-minimized analytics and cross-chain state checks.
    

---

# Quickstart (PoC)

> Requires Python 3.9+.
> 

```bash
python -m pip install -U pip
pip install -e .[cli]

```

## Option A — Use Existing Static Data (most stable for PoC)

Commit your prepared dataset under `apps/web/data/` (including `manifest.json`) and run:

```bash
harborx serve --dir apps/web --port 8080
# Open http://127.0.0.1:8080

```

## Option B — Fetch a Tiny Real Dataset (Blobscan)

If you want to refresh the PoC data from the public API:

```bash
harborx blobscan --limit 3 --out apps/web/data --parquet
harborx manifest --root apps/web --data data --include-parquet
harborx serve --dir apps/web --port 8080

```

Now open the page and click **Rebuild state** to load files. Try queries like:

```sql
SELECT COUNT(*) AS rows FROM state;

SELECT value, COUNT(*) AS c
FROM state
GROUP BY value
ORDER BY c DESC
LIMIT 10;

-- last-write-wins (LWW) per 32-byte chunk (toy example)
WITH ranked AS (
  SELECT value, timestamp, blob_index, position,
         ROW_NUMBER() OVER (ORDER BY timestamp DESC, blob_index DESC, position DESC) rn
  FROM state
)
SELECT value, timestamp
FROM ranked
WHERE rn = 1
LIMIT 50;

```

> The frontend demo runs entirely in the browser via DuckDB-WASM and reads Arrow/Parquet over HTTP. No server database is required.
> 

---

# Unified CLI

All PoC commands are available via the single `harborx` entrypoint:

- **Fetch blobs + write Arrow/Parquet + manifest**
    
    ```bash
    harborx blobscan --limit 3 --out apps/web/data --parquet
    harborx manifest --root apps/web --data data --include-parquet
    
    ```
    
- **Serve static web demo (correct MIME types)**
    
    ```bash
    harborx serve --dir apps/web --port 8080
    
    ```
    
- **Tidy repo (plan vs apply)**
    
    ```bash
    harborx tidy
    harborx tidy --apply
    # (optional) aggressive consolidation into bench/
    harborx tidy --apply --aggressive
    
    ```
    

---

# Frontend-Only Demo

- Location: `apps/web/`
- Data folder: `apps/web/data/`
- Manifest: `apps/web/data/manifest.json` (lists Arrow/Parquet files)

The app resolves file paths **relative to the manifest**. If you ever see 404s like `/data/data/...`, it means both manifest & code added the `data/` prefix. Fix either the manifest (no `data/` prefix) **or** the app’s normalization (strip `data/` if present)—don’t do both.

---

# Repository Layout (Recommended)

```base
harborx/       # unified CLI + data channel (blobscan, tools)
apps/web/      # pure frontend demo (DuckDB-WASM); data/ + manifest.json live here
docs/          # docs site (optional; you can copy apps/web here for Pages)
bench/         # benchmarks (optional; migrate legacy scripts if needed)
legacy/        # archived/older code and experiments

```

Generated artifacts (e.g., large datasets) are typically ignored by Git—except in PoC static mode, where you intentionally commit a tiny `apps/web/data/` for Pages.

---

# Roadmap

- **ZKSQL integration**: verifiable write & query proofs (SNARK-friendly schemas).
- **Connectors**: more sources (e.g., rollup-specific indexers) and push-based ingest.
- **Scaling knobs**: distributed query backends, tiered storage, caching layers.
- **Dev-friendly packaging**: Docker + Compose for “click-to-run” deployments.

---

# License

MIT (PoC). See `LICENSE`.

---

**Questions / Feedback?**

File an issue or ping us with repro steps and logs (CLI args, browser console error, small manifest). Happy to help you get HarborX running smoothly.