#!/usr/bin/env bash
# Shows which cluster processes are running and where the test.users chunks live.
set -uo pipefail
source "$(dirname "$0")/config.sh"

echo "Processes:"
for f in "$DATA_DIR"/*.pid; do
  [ -e "$f" ] || { echo "  none (cluster not started)"; exit 0; }
  name=$(basename "$f" .pid); pid=$(cat "$f")
  if kill -0 "$pid" 2>/dev/null; then echo "  $name  running (pid $pid)"; else echo "  $name  NOT running"; fi
done

msh "$MONGOS_URI" --eval '
  print("\nShards: " + db.adminCommand({listShards: 1}).shards.map(s => s._id).join(", "));
  print("Balancer running: " + sh.getBalancerState());
  const coll = db.getSiblingDB("config").collections.findOne({_id: "test.users"});
  if (!coll) { print("test.users not created yet (run ./baseline.sh)"); quit(0); }
  print("Chunks of test.users:");
  db.getSiblingDB("config").chunks.find({uuid: coll.uuid}).sort({min: 1}).forEach(c =>
    print("  " + EJSON.stringify(c.min.k) + " .. " + EJSON.stringify(c.max.k) + "  on " + c.shard));
' 2>/dev/null || echo "mongos not reachable on port $MONGOS_PORT"
