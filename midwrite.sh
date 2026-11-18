#!/usr/bin/env bash
# Runs the mid-migration write tests and saves the output under results/.
#   ./midwrite.sh              all cases
#   ./midwrite.sh critsec      one case
#   ./midwrite.sh clone,retry  several cases
# Cases: clone, catchup, critsec, commit, after, retry
set -euo pipefail
source "$(dirname "$0")/config.sh"
export CASE="${1:-all}"
mkdir -p "$SCRIPT_DIR/results"
out="$SCRIPT_DIR/results/midwrite-${CASE//,/-}-$(date +%Y%m%d-%H%M%S).txt"
msh "$MONGOS_URI" --file "$DEMO_JS_DIR/midwrite.js" 2>&1 | tee "$out"
echo "Saved to $out"
