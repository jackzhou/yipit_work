#!/usr/bin/env bash
set -e

VENV_PIP=".venv/bin/pip"

# Check venv is valid
if [ ! -f "$VENV_PIP" ]; then
  echo "❌ Virtual environment not found or invalid"
  echo "Run: ./create_venv.sh first"
  exit 1
fi

echo "Using pip: $VENV_PIP"

# Upgrade pip inside venv
$VENV_PIP install --upgrade pip

# Install dependencies safely inside venv
$VENV_PIP install -r requirements.txt

echo "Done!"
