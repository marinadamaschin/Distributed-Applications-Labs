#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p target/classes
javac --release 17 -encoding UTF-8 -d target/classes src/main/java/md/utm/pad/*.java
exec java -cp target/classes md.utm.pad.Main
