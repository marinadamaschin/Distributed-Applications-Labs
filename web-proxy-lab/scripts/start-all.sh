#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/build.sh
mkdir -p logs
pids=()
cleanup() {
  trap - EXIT INT TERM
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
launch() {
  local label="$1"
  shift
  java -cp target/classes md.utm.pad.Main "$@" >"logs/$label.log" 2>&1 &
  pids+=("$!")
  printf '%s PID=%s log=logs/%s.log\n' "$label" "$!" "$label"
}
launch storage storage 8090 data/store.bin
launch dw-1 dw 8081 dw-1
launch dw-2 dw 8082 dw-2
launch proxy proxy 8080
printf 'Four separate JVMs. API: http://127.0.0.1:8080/employees\nCtrl+C stops this group. Wait a few seconds for health checks.\n'
# Portable to macOS Bash 3.2. DW exit is allowed for failover demonstrations.
while kill -0 "${pids[0]}" 2>/dev/null && kill -0 "${pids[3]}" 2>/dev/null; do sleep 1; done
printf 'Storage or proxy exited; inspect logs.\n' >&2
exit 1
