# zkBlob-Lake PoC v16
- Parallel Blob -> Arrow
- Arrow uses fixed_size_binary for addr/key/value/hash
- Query engine switch: --engine {auto,ext,bridge}
- SQLite loader supports --mode {append,upsert}
- Update-ratio sweep to CSV

See scripts/ for usage. Quickstart (Windows/CMD):
  python scripts\cli.py setup
  python scripts\cli.py bench-upd-sweep --ratios 0.1,0.25,0.5 --rows 500000 --parts 4 --workers 4 --chunk 200000 --engine bridge
