#!/usr/bin/env bash
# Turns off every migration failpoint on both shards. Use it if a test was stopped
# (Ctrl+C, error) while a migration was paused, so the migration can finish.
set -euo pipefail
source "$(dirname "$0")/config.sh"
msh "$MONGOS_URI" --eval '
  load(process.env.DEMO_JS_DIR + "/lib.js");
  clearAllFailPoints();
  print("All migration failpoints are off. Any paused migration will now continue.");
'
