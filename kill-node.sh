#!/usr/bin/env bash
# Kills one mongod instantly with SIGKILL, like a power cut: no clean shutdown,
# no chance to finish what it was doing.
#   ./kill-node.sh shard0-primary   kill whichever member of shard0 is primary
#   ./kill-node.sh shard1-2         kill a specific member
# Bring it back with ./restart-node.sh <name>.
set -euo pipefail
source "$(dirname "$0")/config.sh"
target=${1:?usage: ./kill-node.sh <config|shard0|shard1>-primary | <name>}

if [[ $target == *-primary ]]; then
  set=${target%-primary}
  case $set in
    config) uri=$CONFIG_URI ;; shard0) uri=$SHARD0_URI ;; shard1) uri=$SHARD1_URI ;;
    *) echo "unknown replica set: $set" >&2; exit 1 ;;
  esac
  port=$(msh "$uri" --eval 'print(db.hello().me.split(":")[1])')
  case $set in
    config) base=$CONFIG_PORT ;; shard0) base=$SHARD0_PORT ;; shard1) base=$SHARD1_PORT ;;
  esac
  name="$set-$((port - base))"
else
  name=$target
fi

pid=$(cat "$DATA_DIR/$name.pid" 2>/dev/null) || { echo "no pid file for $name" >&2; exit 1; }
kill -KILL "$pid"
echo "killed $name (pid $pid) with SIGKILL"
