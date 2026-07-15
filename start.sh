#!/usr/bin/env bash
# One-command start for TWSE Long PEAD web app.
# Usage:
#   ./start.sh              # create venv if needed, install deps, run
#   PORT=5050 ./start.sh    # custom port (default 5051)
#   HOST=0.0.0.0 ./start.sh # listen on all interfaces
#   OPEN_BROWSER=0 ./start.sh  # do not open browser
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5051}"
OPEN_BROWSER="${OPEN_BROWSER:-1}"
VENV_DIR="${VENV_DIR:-.venv}"
PYTHON_BIN="${PYTHON_BIN:-}"

# Prefer python3.11+ if available; fall back to python3 / python
pick_python() {
  if [[ -n "$PYTHON_BIN" ]]; then
    command -v "$PYTHON_BIN" >/dev/null 2>&1 || {
      echo "error: PYTHON_BIN=$PYTHON_BIN not found" >&2
      exit 1
    }
    echo "$PYTHON_BIN"
    return
  fi
  local c
  for c in python3.13 python3.12 python3.11 python3.10 python3.9 python3 python; do
    if command -v "$c" >/dev/null 2>&1; then
      echo "$c"
      return
    fi
  done
  echo "error: no Python interpreter found (need python3)" >&2
  exit 1
}

ensure_venv() {
  local py
  py="$(pick_python)"
  if [[ ! -d "$VENV_DIR" ]]; then
    echo "→ Creating virtualenv ($VENV_DIR) with $py ..."
    "$py" -m venv "$VENV_DIR"
  fi
  # shellcheck disable=SC1091
  source "$VENV_DIR/bin/activate"
  PY="$VENV_DIR/bin/python"
  if [[ ! -x "$PY" ]]; then
    echo "error: venv python missing at $PY" >&2
    exit 1
  fi
}

deps_ok() {
  "$PY" -c "import flask, requests, pandas, lxml" 2>/dev/null
}

ensure_deps() {
  if deps_ok; then
    return
  fi
  echo "→ Installing dependencies from requirements.txt ..."
  "$PY" -m pip install --upgrade pip -q
  "$PY" -m pip install -r requirements.txt
  if ! deps_ok; then
    echo "error: failed to import required packages after install" >&2
    exit 1
  fi
}

maybe_open_browser() {
  [[ "$OPEN_BROWSER" == "1" ]] || return 0
  local url="http://${HOST}:${PORT}"
  # Open after a short delay so Flask can bind
  (
    sleep 1.2
    if command -v open >/dev/null 2>&1; then
      open "$url" 2>/dev/null || true
    elif command -v xdg-open >/dev/null 2>&1; then
      xdg-open "$url" 2>/dev/null || true
    fi
  ) &
}

export HOST PORT
export TWSE_KILL_PORT="${TWSE_KILL_PORT:-1}"

echo "=== TWSE Long PEAD ==="
echo "  dir:  $ROOT"
ensure_venv
ensure_deps
echo "  py:   $($PY --version 2>&1)"
echo "  url:  http://${HOST}:${PORT}"
echo "  (Ctrl+C to stop)"
echo

maybe_open_browser
exec "$PY" app.py
