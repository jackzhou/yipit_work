#!/usr/bin/env bash

set -e

PYTHON_BIN="/opt/homebrew/bin/python3.11"

echo "Using Python at: $PYTHON_BIN"

if [ ! -f "$PYTHON_BIN" ]; then
  echo "❌ Python 3.11 not found at $PYTHON_BIN"
  echo "Install with: brew install python@3.11"
  exit 1
fi

if [ -d ".venv" ]; then
  echo "✅ .venv already exists, activating..."
else
  echo "Creating virtual environment..."
  $PYTHON_BIN -m venv .venv
fi

# 👉 REQUIRED
source .venv/bin/activate
