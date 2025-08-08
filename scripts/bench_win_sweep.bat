@echo off
REM Sweep update ratios and compare Arrow vs SQLite query times
python scripts\cli.py setup
python scripts\cli.py bench-upd-sweep --ratios 0.1,0.25,0.5 --rows 500000 --parts 4 --workers 4 --chunk 200000 --engine bridge
type bench_upd_sweep.csv
