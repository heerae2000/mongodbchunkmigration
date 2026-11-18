# Shared settings for every script. Override any of these by exporting the
# variable before running a script, for example:
#   SHARD_NODES=1 ./start-cluster.sh
#
# Sourced by the other scripts; not meant to be run directly.

# Folder that holds mongod and mongos. Leave empty to use whatever is on PATH
# (the normal case after installing the mongodb-org packages).
MONGO_BIN="${MONGO_BIN:-}"

# Where data files, logs and pid files go. Delete it to start completely fresh.
DATA_DIR="${DATA_DIR:-$HOME/migration-demo-data}"

# Members per shard replica set. 3 is needed later for node-kill tests;
# 1 uses far less memory if your machine is small.
SHARD_NODES="${SHARD_NODES:-3}"

# Members in the config server replica set.
CONFIG_NODES="${CONFIG_NODES:-1}"

# Seconds the donor waits after a migration before deleting its old copy of the
# moved range (the orphans). Production default is 900 (15 min); 20 keeps the
# orphans around long enough to look at, then lets you watch them disappear.
ORPHAN_DELAY_SECS="${ORPHAN_DELAY_SECS:-20}"

# WiredTiger cache per mongod, in GB. Small, because ~8 processes share one laptop.
WT_CACHE_GB="${WT_CACHE_GB:-0.25}"

# Ports. Chosen away from 27017 so a stray default mongod can't collide.
CONFIG_PORT=27100   # config servers: 27100, 27101, ...
SHARD0_PORT=27110   # shard0 members: 27110, 27111, 27112
SHARD1_PORT=27120   # shard1 members: 27120, 27121, 27122
MONGOS_PORT=27200

# --- derived values (no need to edit below) ---------------------------------

bin() { if [ -n "$MONGO_BIN" ]; then echo "$MONGO_BIN/$1"; else echo "$1"; fi; }

# "host:port,host:port,..." for a replica set starting at $1 with $2 members
members() {
  local base=$1 n=$2 i out=""
  for ((i = 0; i < n; i++)); do out+="localhost:$((base + i)),"; done
  echo "${out%,}"
}

MONGOS_URI="mongodb://localhost:$MONGOS_PORT"
SHARD0_URI="mongodb://$(members $SHARD0_PORT $SHARD_NODES)/?replicaSet=shard0"
SHARD1_URI="mongodb://$(members $SHARD1_PORT $SHARD_NODES)/?replicaSet=shard1"
CONFIG_URI="mongodb://$(members $CONFIG_PORT $CONFIG_NODES)/?replicaSet=config"

# The JavaScript files read these from the environment.
export MONGOS_URI SHARD0_URI SHARD1_URI CONFIG_URI ORPHAN_DELAY_SECS DATA_DIR SHARD_NODES CONFIG_NODES
export CONFIG_PORT SHARD0_PORT SHARD1_PORT

# Server options shared by every mongod.
#  enableTestCommands=1      lets us use configureFailPoint (pausing a migration at a step)
#  orphanCleanupDelaySecs    how long the donor keeps the moved range before deleting it
#  logComponentVerbosity     extra detail from the migration code paths
MONGOD_COMMON=(--bind_ip localhost --fork --wiredTigerCacheSizeGB "$WT_CACHE_GB"
               --setParameter enableTestCommands=1
               --setParameter "orphanCleanupDelaySecs=$ORPHAN_DELAY_SECS"
               --setParameter 'logComponentVerbosity={sharding:{migration:{verbosity:1}, migrationPerf:{verbosity:2}, rangeDeleter:{verbosity:1}}}')

# Start one mongod by name: config-N, shard0-N or shard1-N. The name fixes its
# replica set, role, port, data folder, log file and pid file, so the same call
# starts it the first time and restarts it after a crash.
start_node() {
  local name=$1 set=${1%-*} i=${1##*-} role port
  case $set in
    config) role=configsvr; port=$((CONFIG_PORT + i)) ;;
    shard0) role=shardsvr;  port=$((SHARD0_PORT + i)) ;;
    shard1) role=shardsvr;  port=$((SHARD1_PORT + i)) ;;
    *) echo "unknown node name: $name" >&2; return 1 ;;
  esac
  mkdir -p "$DATA_DIR/$name" "$DATA_DIR/logs"
  "$(bin mongod)" "--$role" --replSet "$set" --port "$port" --dbpath "$DATA_DIR/$name" \
    --logpath "$DATA_DIR/logs/$name.log" --logappend --pidfilepath "$DATA_DIR/$name.pid" \
    "${MONGOD_COMMON[@]}" >/dev/null
  echo "  started $name on port $port"
}

# Mongosh wrapper: quiet output, our helper library preloaded.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DEMO_JS_DIR="$SCRIPT_DIR/js"
msh() { mongosh --quiet --norc "$@"; }
