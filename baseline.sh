#!/usr/bin/env bash
# Runs the baseline migration and saves the output under results/.
set -euo pipefail
source "$(dirname "$0")/config.sh"
mkdir -p "$SCRIPT_DIR/results"
out="$SCRIPT_DIR/results/baseline-$(date +%Y%m%d-%H%M%S).txt"
msh "$MONGOS_URI" --file "$DEMO_JS_DIR/baseline.js" 2>&1 | tee "$out"
echo "Saved to $out"
