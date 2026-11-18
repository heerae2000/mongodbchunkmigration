#!/usr/bin/env bash
# Restarts one mongod that was stopped or killed, with exactly the options it had.
#   ./restart-node.sh shard0-0
# It rejoins its replica set as a secondary and catches up on what it missed.
set -euo pipefail
source "$(dirname "$0")/config.sh"
name=${1:?usage: ./restart-node.sh <config-N|shard0-N|shard1-N>}
if [ -f "$DATA_DIR/$name.pid" ] && kill -0 "$(cat "$DATA_DIR/$name.pid")" 2>/dev/null; then
  echo "$name is already running (pid $(cat "$DATA_DIR/$name.pid"))"
  exit 0
fi
start_node "$name"
