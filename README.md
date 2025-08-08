# zkBlob-Lake PoC (v4)

**What’s new (v4):**
- Windows-safe Arrow writer: explicit file handles + atomic rename with retry (fixes WinError 32)
- Bench works **without** `shell=True`, so SQL like `position < 100000` is safe
- Cross-platform CLI (`scripts/cli.py`) + Windows helper scripts

## Quickstart (Windows / CMD)
```bat
python scripts\cli.py setup
python scripts\cli.py gen --rows 1000000 --parts 1
python scripts\cli.py to-arrow --blob lake\blob_000001.blob.gz --out lake\hot\blob_000001.arrow
python scripts\cli.py q-arrow --sql "SELECT COUNT(*) FROM state WHERE position < 100000"
python scripts\cli.py to-sqlite --blob lake\blob_000001.blob.gz --db lake\sqlite.db
python scripts\cli.py q-sqlite --sql "SELECT COUNT(*) FROM state WHERE position < 100000"
python scripts\cli.py bench --rows 1000000 --sql "SELECT COUNT(*) FROM state WHERE position < 100000"
```

> 如果你坚持用纯 CMD 跑脚本并手写命令，记得在 CMD 里将 `<` 转义为 `^<`；用 CLI 则无需担心。

## Quickstart (PowerShell)
```powershell
python scripts/cli.py setup
python scripts/cli.py gen --rows 1000000 --parts 1
python scripts/cli.py to-arrow --blob lake/blob_000001.blob.gz --out lake/hot/blob_000001.arrow
python scripts/cli.py q-arrow --sql "SELECT COUNT(*) FROM state WHERE position < 100000"
python scripts/cli.py to-sqlite --blob lake/blob_000001.blob.gz --db lake/sqlite.db
python scripts/cli.py q-sqlite --sql "SELECT COUNT(*) FROM state WHERE position < 100000"
python scripts/cli.py bench --rows 1000000 --sql "SELECT COUNT(*) FROM state WHERE position < 100000"
```

