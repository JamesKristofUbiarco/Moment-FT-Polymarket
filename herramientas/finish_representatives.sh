#!/bin/sh
set -eu
cd /home/dirac/Documents/ChatGPT/Zucco/Moment-FT-Polymarket
export PYTHONPATH=src
export OMP_NUM_THREADS=4
export POLARS_MAX_THREADS=8
.venv/bin/python -u herramientas/finish_representatives.py
for day in 2026-04-26 2026-06-09; do
  herramientas/benchmark_dia.sh --date "$day" --tag v5 > "runs/mvp/benchmark_${day}_v5.log" 2>&1
done
.venv/bin/python -c 'from polymarket_mvp.reporting import projection; print(projection("runs/mvp"))'
