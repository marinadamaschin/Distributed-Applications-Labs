#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/build.sh
exec java -cp target/classes md.utm.pad.Main "$@"
