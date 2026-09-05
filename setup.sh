#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python}"

echo "Installing Python dependencies..."
"$PYTHON_BIN" -m pip install -r requirements.txt

echo "Installing Playwright Chromium browser binaries..."
"$PYTHON_BIN" -m playwright install chromium

echo "Setup complete."
