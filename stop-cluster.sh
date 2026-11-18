#!/usr/bin/env bash
# Stops every process started by start-cluster.sh.
#   ./stop-cluster.sh           stop, keep data (start-cluster.sh resumes it)
#   ./stop-cluster.sh --clean   stop and delete all data and logs
set -uo pipefail
source "$(dirname "$0")/config.sh"

# Ask every process in a group to shut down at once, then wait for all of them.
# (One at a time is slow: each replica-set member waits for the others first.)
stop_group() {
  local files=("$@") pids=() names=() f pid i
  for f in "${files[@]}"; do
    [ -e "$f" ] || continue
    pid=$(cat "$f" 2>/dev/null) || continue
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid"; pids+=("$pid"); names+=("$(basename "$f" .pid)")
    fi
    rm -f "$f"
  done
  for _ in $(seq 1 120); do
    local alive=0
    for pid in "${pids[@]}"; do kill -0 "$pid" 2>/dev/null && alive=1; done
    [ $alive = 0 ] && break
    sleep 0.5
  done
  for i in "${!pids[@]}"; do
    kill -0 "${pids[$i]}" 2>/dev/null && kill -KILL "${pids[$i]}"
    echo "  stopped ${names[$i]} (pid ${pids[$i]})"
  done
}

if [ -d "$DATA_DIR" ]; then
  stop_group "$DATA_DIR/mongos.pid"          # router first
  stop_group "$DATA_DIR"/shard*.pid          # then both shards
  stop_group "$DATA_DIR"/config*.pid         # config servers last
fi

if [ "${1:-}" = "--clean" ]; then
  rm -rf "$DATA_DIR"
  echo "Deleted $DATA_DIR"
fi
echo "Cluster stopped."
