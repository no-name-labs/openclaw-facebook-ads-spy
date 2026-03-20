#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "Installing Python Playwright for /ads inspect screenshots..."
"$PYTHON_BIN" -m pip install --upgrade playwright

echo "Installing Chromium for Playwright..."
"$PYTHON_BIN" -m playwright install chromium

echo "Inspect screenshot dependencies are ready."
