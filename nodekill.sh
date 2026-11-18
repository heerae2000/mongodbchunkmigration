#!/usr/bin/env bash
# Runs the node-kill tests and saves the output under results/.
#   ./nodekill.sh                          all scenarios (about 10 minutes)
#   ./nodekill.sh donor-commit             one scenario
#   ./nodekill.sh donor-clone,donor-commit several
# Scenarios: recipient-clone, donor-clone, donor-critsec, donor-commit, config-commit, config-commit-long
set -euo pipefail
source "$(dirname "$0")/config.sh"
export SCENARIO="${1:-all}"
mkdir -p "$SCRIPT_DIR/results"
out="$SCRIPT_DIR/results/nodekill-${SCENARIO//,/-}-$(date +%Y%m%d-%H%M%S).txt"
msh "$MONGOS_URI" --file "$DEMO_JS_DIR/nodekill.js" 2>&1 | tee "$out"
echo "Saved to $out"
