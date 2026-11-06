#!/usr/bin/env bash
# Start a local ClickHouse server from the standalone binary (no Docker).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CLICKHOUSE_BIN:-$ROOT/clickhouse}"
mkdir -p "$ROOT/.chserver" "$ROOT/generator/out"
exec "$BIN" server -- \
  --path="$ROOT/.chserver/" \
  --tmp_path="$ROOT/.chserver/tmp/" \
  --user_files_path="$ROOT/generator/out/" \
  --logger.log="$ROOT/.chserver/server.log" \
  --logger.errorlog="$ROOT/.chserver/error.log" \
  --listen_host=127.0.0.1 \
  --http_port=8123 --tcp_port=9000
