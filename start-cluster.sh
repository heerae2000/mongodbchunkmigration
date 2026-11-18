#!/usr/bin/env bash
# Starts a local sharded cluster:
#   - config server replica set "config"   (CONFIG_NODES members, port 27100+)
#   - shard replica set "shard0"           (SHARD_NODES members, port 27110+)
#   - shard replica set "shard1"           (SHARD_NODES members, port 27120+)
#   - one mongos router                    (port 27200)
# with test commands (failpoints) enabled and the balancer turned off, so the
# only migrations are the ones we start by hand.
set -euo pipefail
source "$(dirname "$0")/config.sh"

for b in mongod mongos; do
  command -v "$(bin $b)" >/dev/null || { echo "Cannot find $(bin $b). Install MongoDB or set MONGO_BIN." >&2; exit 1; }
done
command -v mongosh >/dev/null || { echo "Cannot find mongosh." >&2; exit 1; }

if [ -f "$DATA_DIR/mongos.pid" ] && kill -0 "$(cat "$DATA_DIR/mongos.pid")" 2>/dev/null; then
  echo "Cluster already running (mongos pid $(cat "$DATA_DIR/mongos.pid")). Use ./stop-cluster.sh first."
  exit 0
fi

mkdir -p "$DATA_DIR/logs"
echo "MongoDB: $("$(bin mongod)" --version | head -1)"
echo "Data and logs: $DATA_DIR"

# rs.initiate for a set starting at port $2 with $3 members; $4 = "true" for config servers
initiate() {
  local set=$1 base=$2 n=$3 cfg=$4
  msh "mongodb://localhost:$base/?directConnection=true" --eval "
    const members = [];
    for (let i = 0; i < $n; i++) members.push({_id: i, host: 'localhost:' + ($base + i)});
    try {
      rs.initiate({_id: '$set', configsvr: $cfg, members});
    } catch (e) {
      if (!String(e).includes('already initialized')) throw e;
    }
    // Wait until this set has a primary (any member; after a restart it may not be this one).
    const start = Date.now();
    const hasPrimary = () => { try { return rs.status().members.some(m => m.stateStr === 'PRIMARY'); } catch (e) { return false; } };
    while (!hasPrimary()) {
      if (Date.now() - start > 90000) throw new Error('no primary for $set after 90s');
      sleep(500);
    }
    print('  $set has a primary');
  "
}

echo "Starting config servers..."
for ((i = 0; i < CONFIG_NODES; i++)); do start_node "config-$i"; done
echo "Starting shard0..."
for ((i = 0; i < SHARD_NODES; i++)); do start_node "shard0-$i"; done
echo "Starting shard1..."
for ((i = 0; i < SHARD_NODES; i++)); do start_node "shard1-$i"; done

echo "Initiating replica sets..."
initiate config $CONFIG_PORT $CONFIG_NODES true
initiate shard0 $SHARD0_PORT $SHARD_NODES false
initiate shard1 $SHARD1_PORT $SHARD_NODES false

echo "Starting mongos..."
"$(bin mongos)" --configdb "config/$(members $CONFIG_PORT $CONFIG_NODES)" --port "$MONGOS_PORT" \
  --bind_ip localhost --fork --logpath "$DATA_DIR/logs/mongos.log" --pidfilepath "$DATA_DIR/mongos.pid" \
  --setParameter enableTestCommands=1 >/dev/null
echo "  started mongos on port $MONGOS_PORT"

echo "Adding shards and turning the balancer off..."
msh "$MONGOS_URI" --eval "
  const existing = db.adminCommand({listShards: 1}).shards.map(s => s._id);
  if (!existing.includes('shard0')) sh.addShard('shard0/$(members $SHARD0_PORT $SHARD_NODES)');
  if (!existing.includes('shard1')) sh.addShard('shard1/$(members $SHARD1_PORT $SHARD_NODES)');
  sh.stopBalancer();
  print('  shards: ' + db.adminCommand({listShards: 1}).shards.map(s => s._id).join(', '));
  print('  balancer running: ' + sh.getBalancerState());
"

echo
echo "Cluster is up. Connect with:  mongosh $MONGOS_URI"
echo "Next:  ./baseline.sh"
