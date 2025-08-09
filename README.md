# HarborX — Blob Direct Write & High-Performance SQL Query

**HarborX** is a high-performance data engine for Web3, supporting **direct writes from incremental blockchain Blob data** and efficient SQL querying.
Its goal is to build an **end-to-end data pipeline** — from raw on-chain data to structured formats — and ultimately integrate **zero-knowledge proofs** to enable trusted computation and verification.

## Key Features

* **Blob Direct Write**

  * Write blockchain Blob data directly into structured storage formats without intermediate conversion.
  * Supports both batch and streaming writes to reduce latency and storage overhead.

* **SQL Query Interface**

  * Query the latest blocks and historical data using standard SQL.
  * Queries can run in frontend, backend, or distributed execution environments with flexible deployment.

* **High-Performance Write & Query**

  * Columnar storage with on-demand loading significantly reduces I/O and memory usage.
  * Scales with DataFusion or other distributed query engines for large-scale parallel computation.

* **Verifiable Data Pipeline** *(experimental)*

  * Integrates zero-knowledge proofs to ensure verifiability of both data writes and query results.

## Data Report

We provide `report_unix.sh` / `bench_report.py` to generate performance reports, including:

* Data write speed (rows/sec, MB/s)
* Query latency and throughput
* Storage footprint (by format)

Run:

```bash
./scripts/report_unix.sh
# or
python scripts/bench_report.py
```

Reports are generated in `report.md` with detailed performance metrics.

## Try Online

Try SQL queries instantly:

🌐 [**http://play.harborx.tech/**](http://play.harborx.tech/)

> The online demo uses a compact Parquet dataset and runs SQL queries entirely in the browser for quick testing.

## Common Commands

1. **Generate Demo Dataset**

```bash
python scripts/make_demo_data.py \
    --rows 8000 \
    --parts 4 \
    --update-ratio 0.5 \
    --out-dir web/demo
```

2. **Start Local Preview (Frontend-Only)**

```bash
python -m http.server 8000
# Visit http://127.0.0.1:8000/web/index.html
```

3. **Run SQL Server (Backend Version)**

```bash
python scripts/sql_server.py --port 8000
# Visit http://127.0.0.1:8000/ui
```

4. **Generate Performance Report**

```bash
./scripts/report_unix.sh
# or
python scripts/bench_report.py
```
