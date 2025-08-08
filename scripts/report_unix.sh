#!/usr/bin/env bash
set -euo pipefail

# 选择 Python：优先 .venv，其次系统 python3
if [[ -x ".venv/bin/python" ]]; then
  PYEXE=".venv/bin/python"
else
  PYEXE="$(command -v python3 || command -v python)"
fi
echo "[python] using: $PYEXE"

ROWS="${ROWS:-500000}"
PARTS="${PARTS:-16}"
UPDATE_RATIO="${UPDATE_RATIO:-0.5}"
WORKERS="${WORKERS:-4}"
CHUNK="${CHUNK:-200000}"
WINDOWS="${WINDOWS:-1,4,16,64}"
OUT_MD="${OUT_MD:-report_windows.md}"

$PYEXE scripts/report.py \
  --rows "$ROWS" --parts "$PARTS" \
  --update-ratio "$UPDATE_RATIO" \
  --workers "$WORKERS" --chunk "$CHUNK" \
  --windows "$WINDOWS" \
  --out "$OUT_MD" \
  --no-exact

echo "===== Report ready: $OUT_MD ====="
