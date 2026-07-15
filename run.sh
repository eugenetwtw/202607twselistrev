#!/usr/bin/env bash
# Backward-compatible alias for ./start.sh
exec "$(cd "$(dirname "$0")" && pwd)/start.sh" "$@"
