#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${PLUGIN_ROOT}/.venv"

echo "Creating repo-local Python environment for /ads inspect screenshots..."
if ! "$PYTHON_BIN" -m venv "$VENV_DIR"; then
  cat >&2 <<'EOF'
Failed to create the repo-local virtualenv.

On Ubuntu/Debian, install python venv support first:
  sudo apt-get install -y python3-venv

If your host is still missing ensurepip after that, install the versioned
package too (for example on Ubuntu 24.04):
  sudo apt-get install -y python3.12-venv

Then rerun:
  ./scripts/install-inspect-deps.sh
EOF
  exit 1
fi

VENV_PYTHON="${VENV_DIR}/bin/python"
if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "Repo-local Python binary not found at $VENV_PYTHON" >&2
  exit 1
fi

echo "Installing Python Playwright for /ads inspect screenshots..."
"$VENV_PYTHON" -m pip install --upgrade pip playwright

echo "Installing Chromium for Playwright..."
"$VENV_PYTHON" -m playwright install chromium

echo "Inspect screenshot dependencies are ready."
echo "The plugin will auto-detect ${VENV_PYTHON} on this host."
