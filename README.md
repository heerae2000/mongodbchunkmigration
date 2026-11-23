# Live chunk migration in MongoDB: demo and experiments

These scripts start a small sharded MongoDB cluster on one machine, move a chunk
between shards, and freeze the migration at chosen phases with **failpoints**,
switches built into the server that pause it at an exact line of code. While
the migration is frozen, the scripts write and read through the router and show
where each write landed. They also kill servers partway through a migration while
a workload keeps writing. When the migration finishes, they check that nothing was
lost, duplicated or left visible twice.

Everything runs on the normal MongoDB 8.0 release packages. Nothing needs to be
compiled.

## What you need

- **Linux or WSL (Ubuntu 22.04/24.04), or macOS.** Tested on Linux.
- **MongoDB 8.0 Community Server packages,** which provide `mongod` and `mongos`,
  plus **`mongosh`**. On Ubuntu, install the `mongodb-org` package from MongoDB's
  apt repository; on macOS, run `brew install mongodb-community@8.0`. Check with:
  ```bash
  mongod --version    # db version v8.0.x
  mongosh --version
  ```
- **About 2 GB of free RAM** for the default cluster of 8 processes (it measured 1.3 GB in testing). On a small
  machine, run `SHARD_NODES=1 ./start-cluster.sh` instead, which starts 4 processes.
- **Free ports 27100–27122 and 27200.**

You don't need the MongoDB source code to run the demo. You only need it to read
the code that the output points to (see "Where this happens in the source" below).

## Quick start

```bash
cd migration-demo
./start-cluster.sh      # about 30 s
./baseline.sh           # about 45 s: one ordinary migration
./midwrite.sh           # about 1.5 min: writes at each phase of a migration
./nodekill.sh           # about 6.5 min: kill a primary at each phase, under load
./stop-cluster.sh       # about 30 s: stop everything (add --clean to delete the data)
```

Each test prints `PASS`/`FAIL` lines and ends with `All checks passed.` The
output is also saved under `results/`, which also has example output from
real runs (`results/baseline.txt`, `results/midwrite.txt`, `results/nodekill.txt`).

## The cluster

`start-cluster.sh` starts:

| Component | Name | Ports | Role |
|---|---|---|---|
| Config server replica set | `config` | 27100 | Stores which shard owns which chunk |
| Shard replica set (3 members) | `shard0` | 27110–27112 | The **donor**: owns the data at first |
| Shard replica set (3 members) | `shard1` | 27120–27122 | The **recipient**: receives the chunk |
| Router | `mongos` | 27200 | Where applications send queries |

Every `mongod` starts with these settings:

- **`enableTestCommands=1`** allows `configureFailPoint`, the command that turns
  failpoints on and off.
- **`orphanCleanupDelaySecs=20`** makes the donor delete its old copy of a moved
  chunk after 20 s, instead of the default 15 minutes, so you can watch it happen.
- **Extra log detail** is turned on for the migration and range-deleter code.

The balancer is turned off, so the only migrations are the ones the scripts start.

Each test then creates `test.users`: 100,000 documents
`{k: 0..99999, v: 0, pad: ...}` sharded on `k`, split into two chunks at
`k = 50000`, with both chunks on `shard0`. The test moves the chunk
`[50000, MaxKey)` from `shard0` to `shard1`. Every test starts by recreating
this data.

## The baseline (`./baseline.sh`)

One migration with no pauses and no concurrent writes. The output has six parts:

1. **Starting state:** both chunks on `shard0`.
2. **The move:** a `moveRange` command, usually 2–4 s for this amount of data.
3. **Right after the commit:** the config server says `shard1` owns the range,
   and `shard1` stores all 50,000 documents. `shard0` *also* still stores them
   (the orphans), but queries through `mongos` see each document once.
4. **Phase timings** from `config.changelog`. The donor records six steps and the
   recipient eight. Each line is the time spent getting to that step, so you can
   see where the time goes: almost all of it is the clone, and the critical
   section takes only tens of milliseconds.
5. **Log lines** each shard printed, with their message IDs. You can search the
   source for these IDs (see below).
6. **Orphan cleanup:** about 20 s later the donor's copy shrinks to 0, and the
   range-deleter log lines appear.

## Mid-migration writes (`./midwrite.sh`)

Run every case with `./midwrite.sh`, one case with `./midwrite.sh critsec`, or
several with `./midwrite.sh clone,retry`.

| Case | Where the migration is paused | What the test does | What it shows |
|---|---|---|---|
| `clone` | Recipient, after cloning documents (`migrateThreadHangAtStep4`) | Insert, update and delete in the moving chunk | Writes go to the donor and succeed at once. The recipient's copy is visibly stale until it resumes and catches up. All three writes end up on the recipient. |
| `catchup` | Donor, recipient in steady state (`moveChunkHangAtStep4`) | Same three writes | Writes succeed at once. The recipient applies them **while the donor is still paused**, because in steady state it keeps polling the donor for changes. |
| `critsec` | Donor, inside the critical section (`moveChunkHangAtStep5`) | Read, write with a 3 s limit, write to the *other* chunk, write with no limit | Reads still work. Writes wait, **including writes to the chunk that is not moving**, because the donor's critical section covers the whole collection. A write that hits its time limit is not applied. A write with no limit waits, then lands on the recipient. |
| `commit` | Donor, after the config server commit, before the donor refreshes (`hangBeforePostMigrationCommitRefresh`) | Reads of both chunks | The config server already names the recipient as owner, but **reads now wait too**, because the critical section entered its commit phase just before the commit was sent. A waiting read finishes as soon as the donor is released. |
| `after` | Not paused | Same three writes right after the migration | New writes go only to the recipient. The donor's orphaned copy is stale (`v=0`, deleted documents still present) but never visible through `mongos`. |
| `retry` | Donor, before the critical section (`moveChunkHangAtStep4`) | A retryable `findAndModify $inc` and a retryable insert, then the same commands retried after the migration | The retries go to the new owner and are recognized as already done: the counter is incremented once and the insert isn't duplicated. The donor transferred its record of executed writes along with the documents. A new transaction number does increment again, which shows the check is real. |

How a pause works: the script turns on a failpoint, starts `moveRange` in a
second `mongosh` process (the command blocks while paused), waits until
`$currentOp` reports the migration at that step, does its reads and writes,
then turns the failpoint off and waits for the migration to finish.

## Node kills (`./nodekill.sh`)

Run every scenario with `./nodekill.sh`, or one with `./nodekill.sh donor-commit`.
These tests need 3 members per shard (the default), so a new primary can be elected.

Every scenario follows the same steps:

1. Recreate the data and start a **workload**: a separate `mongosh` process that
   writes continuously through `mongos`, about 100 writes a second, with majority
   write concern and retryable writes. It inserts new documents into the moving
   chunk and into the chunk that stays, and increments a counter. It logs every
   write and whether it was acknowledged.
2. Start the migration and freeze it at a chosen point with a failpoint.
3. Print the **durable recovery records**: the donor's note in
   `config.migrationCoordinators` (with its decision, if any) and the recipient's
   note in `config.migrationRecipients`.
4. **`SIGKILL` a primary**, which stops the process instantly, like a power cut,
   then wait for its replica set to elect a new one.
5. Find the **decision for the interrupted attempt** by its migration ID in the
   donor's log (log 23893, "committed" or "aborted"), and see who owns the range.
6. Restart the killed node, keep writing for 2 s, stop the workload.
7. **Check:** every acknowledged insert is present exactly once; the counter
   includes every acknowledged increment and none twice; the shard that doesn't
   own the range deletes its copy; and, if the range stayed on the donor, a new
   migration succeeds.

Results from a full run (`results/nodekill.txt`):

| Scenario | What is killed, when | Interrupted attempt | Final owner | Longest write stall | Writes lost or doubled |
|---|---|---|---|---|---|
| `recipient-clone` | Recipient primary, after cloning | aborted | shard1 (mongos retried; retry committed) | 7 s | none |
| `donor-clone` | Donor primary, after the recipient cloned | aborted (decided by the new donor primary) | shard1 (mongos retried) | 12 s | none |
| `donor-critsec` | Donor primary, inside the critical section | aborted (new donor primary) | shard1 (mongos retried) | 12 s | none |
| `donor-commit` | Donor primary, after the config commit, before the donor recorded it | **committed** (new donor primary) | shard1 | 10 s | none |
| `config-commit` | Config server as the commit is sent; back after 8 s | committed | shard1 | 11 s | none |
| `config-commit-long` | Config server as the commit is sent; back after 75 s | aborted | shard0 | 61 s (2 writes returned errors and were not applied) | none |

How to read it:

- **The interrupted attempt** is the migration that was running when the server
  died. Its decision is what MongoDB's recovery chose.
- **`mongos` retries `moveRange` by itself.** When an attempt aborts with a
  transient error, `mongos` resends the command (up to 3 attempts) and the retry
  usually succeeds. So `moveRange` often returns `ok` even though the attempt that
  was interrupted aborted. The donor's log shows both attempts.
- **The longest write stall** is the slowest single write the workload saw. Killing
  a primary costs about one election (about 9–10 s here) plus the time to recover.
  The drivers and `mongos` hide it by retrying, so clients see slow writes, not errors.

### Killing a node by hand

For a live demo, you can do the same steps yourself. Start a migration in one
terminal, and in another:

```bash
./kill-node.sh shard0-primary    # SIGKILL whichever shard0 member is primary
./status.sh                      # the killed member shows NOT running
./restart-node.sh shard0-1       # use the name kill-node.sh printed
```

### How recovery decides

At `startClone()` the donor writes a durable note: a document in
`config.migrationCoordinators` with no decision. When a donor member becomes
primary, it looks for such notes:

- **If the note already has a decision,** the new primary just finishes the job.
- **If it doesn't,** the new primary asks the config server who owns the range now.
  If the donor still owns it, the commit never happened, so the decision is
  **aborted**. If the recipient owns it, the commit happened, so the decision is
  **committed**.

The config server's ownership record is the single source of truth. Scenarios
`donor-critsec` and `donor-commit` show the two sides of it: the donor dies in
nearly the same state (critical section held, recipient fully caught up), and the
only difference is whether the config server's commit happened. That alone decides
the outcome.

The same "ask the config server" path runs when the donor's commit request gets
no answer. In `config-commit`, the donor waited inside its critical section until
the config server came back 8 s later, and the commit went through. In
`config-commit-long`, it gave up after about 15 s, left the critical section
without knowing the outcome, and asked the config server every 15 s until it
returned. Then it decided "aborted." Either way, **writes to the whole collection
on the donor waited for the config server**, and in the long outage that was a
full minute.

## What the runs show

- **Writes are never refused during clone and catch-up.** They go to the donor,
  which records them, and the recipient pulls them later. In steady state the
  recipient keeps pulling them even while the donor is paused.
- **The critical section blocks writes to the whole collection on the donor**,
  not only to the moving range. Reads keep working until the commit phase, which
  starts just before the commit is sent to the config server.
- **A write that times out in the critical section is not applied**; nothing is
  half-done.
- **Ownership changes at a single point:** the config server's commit. Before it,
  the donor serves everything. After it, the recipient does, and stale routers
  find out by getting an error from the donor and refreshing.
- **Orphans exist but are invisible** to queries through `mongos`, and they're
  deleted after `orphanCleanupDelaySecs`.
- **Retryable writes stay exactly-once across a migration.**
- **No acknowledged write was lost or applied twice in any node-kill scenario**,
  with majority write concern and retryable writes. Writes with `w: 1` were not
  tested; a killed primary can roll those back, which is a replication effect,
  not a migration one.
- **A crash before the config server commit means abort; after it means commit.**
  The new primary recovers the decision by asking the config server who owns the
  range.
- **The client's answer can hide what happened.** `mongos` retried `moveRange`
  after aborted attempts, so the command returned `ok`. The logs, not the command's
  reply, show that the first attempt aborted.
- **A config server outage during the commit stalls the donor's whole collection**
  until the config server is back, because the donor either waits for the commit
  or, after giving up, needs the config server to settle the outcome.
- **The losing side always cleaned up.** After an abort, the recipient deleted its
  partial copy; after a commit, the donor deleted its orphans.

## Where this happens in the source

Line numbers are for tag **r8.0.34** of `mongodb/mongo`. The donor code is in
`src/mongo/db/s/migration_source_manager.cpp`:

| Lines | What | Failpoint or log ID |
|---|---|---|
| 195–231 | Constructor: checks the request, logs the start | log 22016, `moveChunkHangAtStep1` (231) |
| 402–497 | `startClone()`: starts recording writes, tells the recipient to begin | `moveChunkHangAtStep3` (497) |
| 501–514 | `awaitToCatchUp()`: waits until the recipient is nearly current | `moveChunkHangAtStep4` (514) |
| 518–575 | `enterCriticalSection()`: `_critSec.emplace(... nss() ...)` at 550 blocks writes on the whole collection | log 22017 (569) |
| 577–599 | `commitChunkOnRecipient()`: final catch-up, recipient commits | `moveChunkHangAtStep5` (599) |
| 603–760 | `commitChunkMetadataOnConfig()`: `enterCommitPhase()` at 634 blocks reads, commit sent to the config server, then refresh | `hangBeforePostMigrationCommitRefresh` (673), logs 22018 (724) and 6107801 (745) |
| 788 | After cleanup | `moveChunkHangAtStep6` |

The recipient code is in `src/mongo/db/s/migration_destination_manager.cpp`, in
`_migrateDriver()`, which starts at line 1347:

| Line | Step | Failpoint |
|---|---|---|
| 1375 | Logs "Starting receiving end of chunk migration" (22000) | |
| 1457 | Prior data in range deleted | `migrateThreadHangAtStep1` |
| 1498 | Indexes copied | `migrateThreadHangAtStep2` |
| 1566 | Range-deletion task recorded | `migrateThreadHangAtStep3` |
| 1609 | Documents cloned | `migrateThreadHangAtStep4` |
| 1694 | First catch-up done | `migrateThreadHangAtStep5` |
| 1799 | Steady state ended (donor asked to commit) | `migrateThreadHangAtStep6` |
| 1810 | Final catch-up done | `migrateThreadHangAtStep7` |
| 1839 | Logs "Entered migration recipient critical section" (5899114) | |

Recovery code, also at r8.0.34:

| File : lines | What |
|---|---|
| `src/mongo/db/s/migration_source_manager.cpp` : 489 | `startClone()` calls `_coordinator->startMigration()`, writing the durable note |
| `migration_source_manager.cpp` : 641–668 | Commit request to the config server, sent with a fixed number of retries. If it fails (656), the donor leaves the critical section without a decision (`_cleanup(false)`) and starts `asyncRecoverMigrationUntilSuccessOrStepDown` (666) |
| `migration_source_manager.cpp` : 737 | After a successful commit: `setMigrationDecision(kCommitted)` |
| `migration_source_manager.cpp` : 899 | On an error before the commit: decision `kAborted` |
| `src/mongo/db/s/migration_coordinator.cpp` : 137–160 | `startMigration()`: writes the `config.migrationCoordinators` document and the donor's range-deletion task |
| `migration_coordinator.cpp` : 173–188 | `completeMigration()`: logs 23893 "delivering decision to self and to recipient" |
| `migration_coordinator.cpp` : 221, 320 | Commit and abort paths: make the decision durable, then clean up on both shards |
| `src/mongo/db/s/migration_util.cpp` : 535–573 | `resumeMigrationCoordinationsOnStepUp()`: a new primary finds unfinished notes (logs 4798510, 4798511) |
| `migration_util.cpp` : 575–710 | `recoverMigrationCoordinations()`: decision already known (604), or not known, so ask the config server (612); still own the range means aborted, otherwise committed (695–698) |
| `migration_util.cpp` : 791–837 | `resumeMigrationRecipientsOnStepUp()`: the recipient restores its critical section from `config.migrationRecipients` |
| `migration_util.cpp` : 857 | `asyncRecoverMigrationUntilSuccessOrStepDown()`: keeps retrying recovery in the background |
| `src/mongo/s/commands/cluster_move_range_cmd.cpp` : 101–108 | `mongos` sends `moveRange` to the config server with `RetryPolicy::kIdempotent`, which retries transient errors (up to 3 attempts, `kOnErrorNumRetries` in `src/mongo/s/client/shard.cpp`:51) |

To find any log line from the output in the source, search for its ID, for
example `grep -rn "22017" src/mongo/db/s/`.

The scripts were tested against MongoDB 8.0.23. If your patch version differs,
the behavior is the same, but confirm line numbers against your checked-out tag.

## Files

| File | Purpose |
|---|---|
| `config.sh` | Ports, folders and settings shared by every script |
| `start-cluster.sh` | Start the cluster with failpoints enabled and the balancer off |
| `stop-cluster.sh` | Stop it (`--clean` also deletes data and logs) |
| `status.sh` | Show which processes are up and where the chunks are |
| `baseline.sh`, `js/baseline.js` | Baseline migration |
| `midwrite.sh`, `js/midwrite.js` | Mid-migration write cases |
| `nodekill.sh`, `js/nodekill.js` | Node-kill scenarios with a live workload and checker |
| `kill-node.sh` | `SIGKILL` one node by name, or a set's current primary (`./kill-node.sh shard0-primary`) |
| `restart-node.sh` | Restart a killed node with its original options (`./restart-node.sh shard0-1`) |
| `unpause.sh` | Turn off all failpoints if a test was interrupted mid-pause |
| `js/lib.js` | Shared helpers: failpoints, waiting for steps, background commands, inspecting shards, killing and restarting nodes, recovery records, workload and checker |

Data and logs go to `~/migration-demo-data/`. Full server logs are in
`~/migration-demo-data/logs/<node>.log`, one JSON object per line.

## Settings

Export a setting before running a script to change it:

| Variable | Default | Meaning |
|---|---|---|
| `SHARD_NODES` | 3 | Members per shard replica set (1 saves memory; 3 is needed for node-kill tests) |
| `CONFIG_NODES` | 1 | Members in the config server replica set (with 3, the config scenarios kill the primary and an election follows; not tested) |
| `ORPHAN_DELAY_SECS` | 20 | Seconds before the donor deletes the moved range |
| `DOCS` | 100000 | Documents loaded for each test |
| `MONGO_BIN` | (empty) | Folder containing `mongod`/`mongos`, if they aren't on your PATH |
| `DATA_DIR` | `~/migration-demo-data` | Where data, logs and pid files go |

`SHARD_NODES`, `CONFIG_NODES`, `ORPHAN_DELAY_SECS` and `DATA_DIR` take effect at
`./start-cluster.sh`. To change them, stop with `--clean` and start again.

## Troubleshooting

- **"Cannot find mongod":** MongoDB isn't on your PATH. Install the packages, or
  set `MONGO_BIN=/path/to/folder/with/mongod`.
- **"Address already in use" or a node fails to start:** something else is using
  the ports. Run `./stop-cluster.sh`, then check with
  `ss -ltnp | grep -E '271[0-2][0-9]|27200'`.
- **A test was interrupted and the next test hangs:** a migration is still paused
  at a failpoint. Run `./unpause.sh`.
- **A node-kill test was interrupted and a node is still down:** `./status.sh`
  shows it as `NOT running`. Bring it back with `./restart-node.sh <name>`.
- **A test fails or the cluster behaves oddly:** start fresh with
  `./stop-cluster.sh --clean && ./start-cluster.sh`.
- **To see why something failed,** look in `~/migration-demo-data/logs/` at
  `shard0-0.log` (usually the donor primary), `shard1-0.log` (usually the recipient primary) and
  `mongos.log`.
