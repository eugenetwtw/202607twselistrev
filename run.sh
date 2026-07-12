#!/bin/zsh
# 啟動 Long PEAD 網頁（會清掉佔用 5050 的舊行程）
set -e
cd "$(dirname "$0")"
PORT="${PORT:-5051}"
if command -v lsof >/dev/null 2>&1; then
  for pid in $(lsof -ti ":$PORT" 2>/dev/null); do
    echo "Killing pid $pid on :$PORT"
    kill -9 "$pid" 2>/dev/null || true
  done
  sleep 0.3
fi
echo "Starting from $(pwd)"
exec python3 app.py
