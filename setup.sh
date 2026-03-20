#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Trading System — Setup Script (Conda)
# Creates a conda env with Python 3.14 free-threaded (no GIL)
# ─────────────────────────────────────────────────────────────────────────────
set -e

ENV_NAME="trading-system"

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Trading System — Conda Environment Setup"
echo "═══════════════════════════════════════════════════════"

# ── Verify conda is available ─────────────────────────────────────────────────
if ! command -v conda &>/dev/null; then
    echo "ERROR: conda not found."
    echo "Install Miniconda: https://docs.conda.io/en/latest/miniconda.html"
    exit 1
fi
echo "Conda       : $(conda --version)"

# ── Create or update environment ─────────────────────────────────────────────
if conda env list | grep -q "^${ENV_NAME} "; then
    echo "Env '${ENV_NAME}' already exists — updating..."
    conda env update -n "$ENV_NAME" -f environment.yml --prune
else
    echo "Creating conda env '${ENV_NAME}' with Python 3.12..."
    conda env create -f environment.yml
fi

# ── Activate & verify ─────────────────────────────────────────────────────────
eval "$(conda shell.bash hook)"
conda activate "$ENV_NAME"

PYTHON=$(conda run -n "$ENV_NAME" which python)
echo "Python      : $(conda run -n "$ENV_NAME" python --version)"

GIL_STATUS=$(conda run -n "$ENV_NAME" python -c \
    "import sys; print('DISABLED ✓ (free-threaded)' if hasattr(sys,'_is_gil_enabled') and not sys._is_gil_enabled() else 'enabled (standard build)')")
echo "GIL status  : $GIL_STATUS"

# ── Config ────────────────────────────────────────────────────────────────────
if [ ! -f "config.yaml" ]; then
    cp config.example.yaml config.yaml
    echo "Created config.yaml — edit it before running live."
fi

# ── Directories ───────────────────────────────────────────────────────────────
mkdir -p logs data/historical backtest_results

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Setup complete!"
echo "═══════════════════════════════════════════════════════"
echo ""
echo "  Activate env  : conda activate ${ENV_NAME}"
echo "  Run tests     : conda run -n ${ENV_NAME} pytest tests/ -v --cov=src"
echo "  Run backtest  : conda run -n ${ENV_NAME} python backtest.py"
echo "  Run live      : conda run -n ${ENV_NAME} python main.py"
echo "  No-GIL mode   : conda run -n ${ENV_NAME} env PYTHON_GIL=0 python main.py"
echo ""
