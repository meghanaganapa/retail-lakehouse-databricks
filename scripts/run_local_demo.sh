#!/usr/bin/env bash
# Run the whole lakehouse locally: two arrival batches, then the quality gate.
#   pip install -e ".[dev]"
#   ./scripts/run_local_demo.sh
set -euo pipefail

OUT="${OUT:-./demo}"
rm -rf "$OUT"
mkdir -p "$OUT"

echo ">>> Batch 1: data for 1-14 Sep lands"
generate-data --out "$OUT/landing" --batch 1
run-pipeline --landing "$OUT/landing" --base-path "$OUT/lakehouse" --summary-json "$OUT/batch1.json"

echo ">>> Batch 2: 15-21 Sep, plus late events, re-sent rows and a supplier schema change"
generate-data --out "$OUT/landing" --batch 2
run-pipeline --landing "$OUT/landing" --base-path "$OUT/lakehouse" --summary-json "$OUT/batch2.json"

echo ">>> Quality gate"
quality-report --base-path "$OUT/lakehouse"
