@echo off
REM Windows full demo using CLI (safe for '<')
set SQL=SELECT COUNT(*) FROM state WHERE position ^< 100000
python scripts\cli.py setup
python scripts\cli.py gen --rows 1000000 --parts 1
python scripts\cli.py to-arrow --blob lake\blob_000001.blob.gz --out lake\hot\blob_000001.arrow
python scripts\cli.py q-arrow --duckdb --sql "%SQL%"
python scripts\cli.py to-sqlite --blob lake\blob_000001.blob.gz --db lake\sqlite.db
python scripts\cli.py q-sqlite --sql "%SQL%"
python scripts\cli.py bench --rows 1000000 --sql "%SQL%"
