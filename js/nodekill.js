// Node-kill tests: start a migration under a continuous write workload, freeze it at a
// chosen point, SIGKILL a primary, and check what the cluster does:
//   - who owns the range afterwards (did the migration commit or abort?)
//   - whether every acknowledged write survived exactly once
//   - whether the shard that lost (or never got) the range cleans up its copy
//   - whether the cluster can migrate again afterwards
//
// Run one scenario with SCENARIO=<name>, several with a comma list, or all (the default):
//   recipient-clone   recipient primary killed after cloning, before catch-up
//   donor-clone       donor primary killed at the same point
//   donor-critsec     donor primary killed inside the critical section, before the commit
//   donor-commit      donor primary killed after the config server commit, before the donor
//                     has recorded the decision
//   config-commit     config server killed just as the donor sends the commit, back after 8 s
//   config-commit-long  the same, but the config server stays down for 75 s
load(process.env.DEMO_JS_DIR + "/lib.js");

const SHARD_NODES = Number(process.env.SHARD_NODES);
const CONFIG_NODES = Number(process.env.CONFIG_NODES);

// Recovery logs (step-up recovery, decision) are at debug level 2 of the migration component.
// Raise it on every running shard member so whichever member becomes primary logs them.
function raiseRecoveryLogging() {
  for (const set of ["shard0", "shard1"]) {
    for (let i = 0; i < SHARD_NODES; i++) {
      try {
        const c = new Mongo(`mongodb://localhost:${SET_BASE_PORT[set] + i}/?directConnection=true`);
        c.getDB("admin").runCommand({setParameter: 1, logComponentVerbosity: {sharding: {migration: {verbosity: 2}}}});
        c.close();
      } catch (e) { /* member down; it will not be the one we read */ }
    }
  }
}

// Logs worth showing after a kill, from the migration component, on the current primary.
const RECOVERY_IDS = new Set([4798510, 4798511, 4798501, 4798502, 23891, 23893, 23894, 23899, 4798503, 6064504,
  5899102, 6064505, 22016, 22017, 22018, 22000, 22011, 21998, 5899114, 5899108, 6316100, 4817410, 6107802,
  7627801, 6386803, 5004703, 23893]);
function printRecoveryLogs(title, conn, since) {
  print(`\n  -- ${title} --`);
  let rows = [];
  try {
    rows = migrationLogs(conn, since, ["MIGRATE", "SHARDING"]).filter(e => RECOVERY_IDS.has(e.id) || e.s === "W" || e.s === "E");
  } catch (e) { print("  (could not read log: " + e.message + ")"); return; }
  if (!rows.length) print("  (no matching log lines)");
  for (const e of rows) {
    const a = e.attr || {};
    const extra = ["decision", "error", "reason"].filter(k => a[k] !== undefined)
      .map(k => `${k}=${typeof a[k] === "object" ? (a[k].codeName || a[k].errmsg || JSON.stringify(a[k])) : a[k]}`);
    print(`  ${clock(new Date(e.t.$date))}  id=${String(e.id).padEnd(8)} ${e.msg}` + (extra.length ? `  [${extra.join(", ").slice(0, 110)}]` : ""));
  }
}

// The decision for one migration attempt, found by its migrationId in the donor members'
// log files: log 23893 "MigrationCoordinator delivering decision" says committed or aborted,
// and which member logged it (the original primary, or the one that recovered it).
function findDecision(migrationIdHex) {
  const fs = require("fs");
  for (let i = 0; i < SHARD_NODES; i++) {
    const file = `${process.env.DATA_DIR}/logs/${DONOR}-${i}.log`;
    if (!fs.existsSync(file)) continue;
    for (const line of fs.readFileSync(file, "utf8").split("\n")) {
      if (!line.includes('"id":23893')) continue;
      const e = JSON.parse(line);
      // Logged as {migrationId: {uuid: {$uuid: "d9125ad4-..."}}}
      const m = (e.attr && e.attr.migrationId) || {};
      const id = (m.uuid && m.uuid.$uuid) || m.$uuid || "";
      if (id && id.replace(/-/g, "") === migrationIdHex)
        return {decision: e.attr.decision, node: `${DONOR}-${i}`, at: new Date(e.t.$date)};
    }
  }
  return null;
}

// ------------------------------------------------------------------------------------
// One scenario, in the same shape every time.
//   s.title, s.expect     printed at the start
//   s.set                 replica set whose primary is killed ("shard0", "shard1", "config")
//   s.pause()             turn on the failpoint(s) that freeze the migration
//   s.waitPaused()        block until the migration is frozen there
//   s.afterKill()         optional: release failpoints on the side that survived
//   s.expectDecision      "committed", "aborted", or null (either acceptable) for the attempt
//                         that was interrupted. mongos may then retry moveRange by itself
//                         (up to 3 attempts on transient errors), so the final owner can differ.
function runScenario(s) {
  heading(`SCENARIO ${s.name}: ${s.title}`);
  for (const line of s.expect) print(line);
  print("");
  if (s.set !== "config" && SHARD_NODES < 3) {
    print(`  SKIPPED: needs 3 members per shard so a new primary can be elected (SHARD_NODES=${SHARD_NODES}).`);
    return;
  }

  resetData();
  raiseRecoveryLogging();
  const events = [];
  const w = startWorkload();
  sleep(2000);

  s.pause();
  const mig = startMigration();
  s.waitPaused();
  events.push({at: new Date(), label: "paused"});
  printOwners();
  printRecoveryRecords("Durable recovery records just before the kill");
  const migrationId = coordinatorDocs()[0]._id.toString("hex");

  // --- the kill
  const killed = killPrimary(s.set);
  events.push({at: killed.at, label: "KILL"});
  if (s.afterKill) s.afterKill();

  let downNode = killed.name;
  if (s.set === "config" && CONFIG_NODES === 1) {
    // No other member to take over: the config server is simply down until restarted.
    const downSecs = s.configDownSecs || 8;
    say(`Config server has no other member; it stays down for ${downSecs} s, then is restarted`);
    sleep(downSecs * 1000);
    restartNode(killed.name);
    events.push({at: new Date(), label: "restart"});
    downNode = null;
    waitForSetHealthy("config");
  } else {
    waitForNewPrimary(s.set, killed.name);
    events.push({at: new Date(), label: "elected"});
  }

  // --- outcome
  const m = waitForMigration(mig);
  const migrationReturnedOk = m.res.ok === 1;
  waitForRecoveryRecordsCleared();
  events.push({at: new Date(), label: "settled"});
  print("");
  const dec = findDecision(migrationId);
  if (dec) say(`Interrupted attempt (migrationId ${migrationId.slice(0, 8)}…) was decided ${dec.decision.toUpperCase()}, ` +
               `logged by ${dec.node}${dec.node === killed.name ? "" : (s.set === DONOR ? " (the newly elected donor primary)" : "")}`);
  else say(`No decision found in the donor logs for migrationId ${migrationId.slice(0, 8)}…`);
  if (s.expectDecision) check(`interrupted attempt was ${s.expectDecision}`, dec && dec.decision === s.expectDecision);
  else check("interrupted attempt reached a decision", !!dec);

  printOwners();
  const owner = chunkOwners().find(l => l.startsWith(`[${SPLIT},`)).split(" on ")[1];
  const retried = dec && dec.decision === "aborted" && owner === RECIPIENT;
  say(`Final owner of the range: ${owner}` +
      (retried ? ` (mongos retried moveRange automatically, and the retry committed)` : "") +
      `; moveRange returned ${migrationReturnedOk ? "ok" : "an error: " + (m.res.codeName || m.res.errmsg || "")}`);
  check(`range has exactly one owner`, owner === DONOR || owner === RECIPIENT);
  check(`moveRange's reply matches the outcome (${migrationReturnedOk ? "ok and" : "error and"} owner ${owner})`,
        migrationReturnedOk === (owner === RECIPIENT));

  printRecoveryLogs(`What the ${s.set === "config" ? "donor" : s.set} primary logged after the kill`,
                    s.set === RECIPIENT ? recipientConn : donorConn, killed.at);

  // --- bring the killed node back, keep writing a little longer, stop the workload
  if (downNode) {
    restartNode(downNode);
    events.push({at: new Date(), label: "restart"});
    waitForSetHealthy(s.set);
  }
  sleep(2000);
  const recs = stopWorkload(w);

  heading(`SCENARIO ${s.name}: what clients saw, and the checker`);
  summarizeWorkload(recs, events);
  checkWorkload(recs);

  print("");
  const r = waitForNonOwnerEmpty();
  check(`${r.nonOwner} (not the owner) deleted its copy of the range`, r.empty);
  const c = printCounts("final");
  check("mongos sees each document in the range exactly once", c.mongos === c[owner === DONOR ? "donor" : "recipient"]);

  if (owner === DONOR) {
    say("Range is still on the donor; migrating it again to show the cluster is healthy:");
    const res = mongosConn.getDB("admin").runCommand({moveRange: NS, min: {k: SPLIT}, toShard: RECIPIENT});
    check("a new migration of the same range succeeds", res.ok === 1);
  }
}

// ------------------------------------------------------------------------------------

const SCENARIOS = {};

SCENARIOS["recipient-clone"] = {
  title: "kill the RECIPIENT primary after it has cloned the documents",
  expect: [
    "Expected: the interrupted attempt aborts. The donor never stopped being the owner, so it",
    "keeps serving writes. The donor records the abort and marks the recipient's range-deletion",
    "task ready, so the recipient's partial copy is deleted. mongos may then retry the moveRange",
    "against shard1's new primary.",
  ],
  set: "shard1",
  expectDecision: "aborted",
  pause() { pauseRecipientAt(4); },
  waitPaused() { waitForRecipientStep(4); },
};

SCENARIOS["donor-clone"] = {
  title: "kill the DONOR primary after the recipient has cloned the documents",
  expect: [
    "Expected: the interrupted attempt aborts. Writes stall while shard0 elects a new",
    "primary (about 10 s), then resume. The new donor primary finds the unfinished migration in",
    "config.migrationCoordinators with no decision, asks the config server who owns the range,",
    "learns it is still the owner, and records the decision as aborted. mongos may then retry",
    "the moveRange against the new donor primary.",
  ],
  set: "shard0",
  expectDecision: "aborted",
  pause() { pauseRecipientAt(4); },
  waitPaused() { waitForRecipientStep(4); },
  // The recipient is still frozen at its failpoint; let it go so it notices the donor is gone.
  afterKill() { resumeRecipientAt(4); },
};

SCENARIOS["donor-critsec"] = {
  title: "kill the DONOR primary inside the critical section, before the commit",
  expect: [
    "Expected: the interrupted attempt aborts, even though the recipient already holds a complete copy and",
    "its own critical section. The commit never reached the config server, so the config server",
    "still lists the donor as owner, and recovery decides from that. mongos may then retry the",
    "moveRange against the new donor primary.",
  ],
  set: "shard0",
  expectDecision: "aborted",
  pause() { pauseDonorAt(5); },
  waitPaused() { waitForDonorStep(5); },
};

SCENARIOS["donor-commit"] = {
  title: "kill the DONOR primary after the config server commit, before the donor records it",
  expect: [
    "Expected: the migration COMMITS. The donor died before writing the decision into",
    "config.migrationCoordinators, but the config server already lists the recipient as owner.",
    "The new donor primary asks the config server, sees it no longer owns the range, records the",
    "decision as committed, and later deletes its orphans.",
  ],
  set: "shard0",
  expectDecision: "committed",
  pause() {
    setFailPoint(donorConn, "hangBeforePostMigrationCommitRefresh", true);
    say("Donor will pause right after the config server commit (failpoint hangBeforePostMigrationCommitRefresh)");
  },
  waitPaused() {
    const t = Date.now();
    while (!chunkOwners().some(l => l.startsWith(`[${SPLIT}, MaxKey) on ${RECIPIENT}`))) {
      if (Date.now() - t > 60000) throw new Error("commit never happened");
      sleep(100);
    }
    sleep(300);
    say("Config server has committed the new owner; donor is paused before recording the decision");
  },
};

SCENARIOS["config-commit"] = {
  title: "kill the CONFIG SERVER just as the donor sends the commit; back after 8 s",
  expect: [
    "Expected: the donor's commit request cannot reach a config server, so the donor waits for",
    "one to come back, still inside its critical section. Writes to the whole collection on the",
    "donor wait the entire time. When the config server returns, the commit goes through.",
  ],
  set: "config",
  configDownSecs: 8,
  expectDecision: CONFIG_NODES === 1 ? "committed" : null,
  pause() { pauseDonorAt(5); },
  waitPaused() { waitForDonorStep(5); },
  // Release the donor so it sends its commit to a config server that is gone.
  afterKill() { resumeDonorAt(5); },
};

SCENARIOS["config-commit-long"] = {
  title: "kill the CONFIG SERVER just as the donor sends the commit; back after 75 s",
  expect: [
    "Expected: the donor gives up waiting for an answer, leaves the critical section without",
    "knowing the outcome, and then keeps asking the config server who owns the range: the same",
    "recovery path as after a donor crash. The commit never reached the config server, so the",
    "migration aborts.",
  ],
  set: "config",
  configDownSecs: 75,
  expectDecision: CONFIG_NODES === 1 ? "aborted" : null,
  pause() { pauseDonorAt(5); },
  waitPaused() { waitForDonorStep(5); },
  afterKill() { resumeDonorAt(5); },
};

for (const [name, s] of Object.entries(SCENARIOS)) s.name = name;

const order = Object.keys(SCENARIOS);
const which = (process.env.SCENARIO || "all").toLowerCase();
const toRun = which === "all" ? order : which.split(",");
for (const name of toRun) {
  if (!SCENARIOS[name]) { print(`Unknown scenario "${name}". Choose from: ${order.join(", ")}, all`); quit(2); }
  try {
    runScenario(SCENARIOS[name]);
  } catch (e) {
    check(`scenario ${name} ran to completion (${e.message})`, false);
    try { clearAllFailPoints(); } catch (_) {}
  }
}
finish();
