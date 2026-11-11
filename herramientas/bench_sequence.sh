#!/bin/sh
set -eu
cd /home/dirac/Documents/ChatGPT/Zucco/Moment-FT-Polymarket
while tmux has-session -t polymarket-benchmark 2>/dev/null; do sleep 5; done
herramientas/benchmark_dia.sh --date 2026-06-06 --tag v2 > runs/mvp/benchmark_fast.log 2>&1
herramientas/benchmark_dia.sh --date 2026-06-06 --mode remote --tag v2 > runs/mvp/benchmark_remote.log 2>&1
for day in 2026-02-26 2026-04-26 2026-06-09; do
  while [ ! -f "runs/mvp/staging/$day.parquet" ]; do sleep 5; done
  herramientas/benchmark_dia.sh --date "$day" --tag v2 > "runs/mvp/benchmark_${day}_v2.log" 2>&1
done
