# **HarborX — Efficient Web3 Data Engine**

HarborX is **data-source–centric**. It lets you subscribe to, compose, process, and publish data sources—avoiding repeated collection/indexing/cleaning—so you can focus on your product. HarborX uses a modular design to allow custom data pipelines, and a unified intermediate model that works across heterogeneous data origins, databases, and query engines.

### Core Capabilities

- **Verifiable Blob Direct-Write**: Efficiently materializes **ZK Rollup** blobs into Parquet, retains provenance proofs for data verification, and enables SQL queries over on-chain state for all integrated ZK Rollups.
- **Subscribe & Catch-Up**: Developers subscribe to data sources as easily as installing an `npm` package. HarborX uses **Mirror** to auto-fetch and continuously catch up, and leverages **`AT commit_id`** to provide a consistent, replayable query view.
- **Modularity**:
    - **Upstream**: Use `SourceAdapter` to support join queries across heterogeneous sources such as ZK Rollups, indexers, Farcaster, etc.
    - **Downstream**: Use `Gateway/Materializer` to target different databases and query engines (DuckDB/PG/CH, etc.).
- **Publish & Reuse**: Publish derived results as new data sources; others can subscribe and perform cross-source joins—compounding reusable ecosystem assets and reducing duplicated indexing work.
- **Open Distribution**: Persist data to decentralized storage, and broadcast pointer events over a **P2P** network to reduce single-point dependency and platform lock-in.

## **Quickstart (PoC)**

> Requires Python 3.9+.
> 

```bash
python -m pip install -U pip
pip install -e .[cli]
```

### Add a Data Source

Add a data source with one command:

```bash
harborx add --base https://play.harborx.tech/data/ --subdir local -m --port 8080
```

You’ll see output like:

```bash
harborx add --base https://play.harborx.tech/data/ --subdir local -m --port 8080
[2025-08-13 23:46:03] 🔗 Using Lake base: https://play.harborx.tech/data
[2025-08-13 23:46:03] ⬇️  Fetching manifest.json from https://play.harborx.tech/data
[2025-08-13 23:46:04] 🧭 Rewriting relative paths → absolute URLs
[2025-08-13 23:46:04] 📄 No local manifest found, adopting remote manifest as local
[2025-08-13 23:46:04] 🧱 Materializing → HarborX\apps\web\data\local\objects
[2025-08-13 23:46:06]   ↓ https://play.harborx.tech/data/1456cfb63a334a39a06df3ee120daafb-0.arrow
    24cfd00e87ded65b.arrow done in 7.3s
[2025-08-13 23:46:17]   ↓ https://play.harborx.tech/data/2bb64d90458245e990c29acd605dadce-0.arrow
    db3c961ccec074e1.arrow done in 21.7s
[2025-08-13 23:52:05]   ↓ https://play.harborx.tech/data/chain_id=167001/date=20310/topic=state_diff/2bb64d90458245e990c29acd605dadce-0.parquet
    b2089242e6daa02b.parquet done in 1.7s
[2025-08-13 23:52:12]   ↓ https://play.harborx.tech/data/chain_id=167001/date=20310/topic=state_diff/7721bc175b1a4194a04f2779536f0d15-0.parquet
    4a1aea6b788713e2.parquet done in 1.5s
[2025-08-13 23:52:14] 🔍 Validating a few entries
[2025-08-13 23:52:14]   ✅ local OK: objects/24cfd00e87ded65b.arrow
[2025-08-13 23:52:14]   ✅ local OK: objects/db3c961ccec074e1.arrow
[2025-08-13 23:52:14]   ✅ local OK: objects/360ce304038b70f4.parquet
[2025-08-13 23:52:14]   ✅ local OK: objects/d7e92bfb8c72c3ac.parquet
[2025-08-13 23:52:14] 📊 Merge summary:
  • arrow:   2 items
  • parquet: 2 items
  • files:   0 items
  • segments:0 items
  (Δ added) arrow:+2 parquet:+2 files:+0 segments:+0
  Probed OK:4 FAIL:0
[2025-08-13 23:52:14] 🚀 Starting harborx static server at http://127.0.0.1:8080
[2025-08-13 23:52:14]     Serving directory: HarborX\apps\web (manifest at HarborX\apps\web\data\local\manifest.json)
[serve] http://127.0.0.1:8080 (root=HarborX\apps\web)
```

This is a PoC example; the production version will automatically catch up and stay in sync.

## Use Existing Static Data

Commit your prepared dataset under `apps/web/data/` (including `manifest.json`) and run:

```bash
harborx serve --dir apps/web --port 8080
# Open http://127.0.0.1:8080

```

## Fetch a Tiny Real Dataset

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

# CLI

All commands are available via the single `harborx` entrypoint:

- **Fetch blobs + write Arrow/Parquet + manifest**
    
    ```bash
    harborx blobscan --limit 3 --out apps/web/data --parquet
    harborx manifest --root apps/web --data data --include-parquet
    ```
    
- **Serve static web demo**
    
    ```bash
    harborx serve --dir apps/web --port 8080
    ```

---

# Frontend Demo

- Location: `apps/web/`
- Data folder: `apps/web/data/`
- Manifest: `apps/web/data/manifest.json` (lists Arrow/Parquet files)

The app resolves file paths **relative to the manifest**. 

---

# Repository Layout

```base
harborx/       # unified CLI + data channel (blobscan, tools)
apps/web/      # pure frontend demo (DuckDB-WASM); data/ + manifest.json live here
docs/          # docs site (optional; you can copy apps/web here for Pages)
bench/         # benchmarks (optional; migrate legacy scripts if needed)
legacy/        # archived/older code and experiments

```

---

# License

Apache-2.0. See `LICENSE`.