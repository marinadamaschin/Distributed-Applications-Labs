#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo 'Running gRPC broker test suite (unit + reliability + integration, when present)...'
mvn --batch-mode clean test