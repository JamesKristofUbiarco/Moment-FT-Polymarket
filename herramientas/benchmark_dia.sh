#!/bin/sh
set -eu
cd /home/dirac/Documents/ChatGPT/Zucco/Moment-FT-Polymarket
export PYTHONPATH=src
export POLARS_MAX_THREADS=8
export OMP_NUM_THREADS=8
exec .venv/bin/python -u -m polymarket_mvp.runner "$@"
