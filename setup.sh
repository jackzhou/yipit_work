#!/usr/bin/env bash

set -e  # stop on error

PYTHON_BIN="/opt/homebrew/bin/python3.11"

echo "Using Python at: $PYTHON_BIN"

if [ ! -f "$PYTHON_BIN" ]; then
  echo "❌ Python 3.11 not found at $PYTHON_BIN"
  echo "Install with: brew install python@3.11"
  exit 1
fi

echo "Creating virtual environment (.venv) with Python 3.11..."
$PYTHON_BIN -m venv .venv

echo "Activating virtual environment..."
source .venv/bin/activate

echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing dependencies..."

pip install \
  pandas>=2.0 \
  numpy>=1.24 \
  duckdb>=0.10 \
  pyarrow>=14.0 \
  sentence-transformers>=2.2 \
  scikit-learn>=1.3 \
  python-dateutil>=2.8 \
  rapidfuzz>=3.0

echo "Done!"
echo "To activate later: source .venv/bin/activate"
echo "Python version in venv:"
python --version
