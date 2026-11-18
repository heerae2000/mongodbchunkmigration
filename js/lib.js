// Shared helpers for the migration experiments. Loaded by the other scripts with
//   load(process.env.DEMO_JS_DIR + "/lib.js")
//
// The experiment always moves the same chunk:
//   collection test.users, shard key {k: 1}, documents k = 0 .. DOCS-1
//   chunk [SPLIT, MaxKey) moves from shard0 (donor) to shard1 (recipient)

const DB_NAME = "test";
const COLL_NAME = "users";
const NS = DB_NAME + "." + COLL_NAME;
const DOCS = Number(process.env.DOCS || 100000);
const SPLIT = DOCS / 2;
const DONOR = "shard0";
const RECIPIENT = "shard1";

// Connections. A replica-set URI connects to that set's current primary.
const mongosConn = new Mongo(process.env.MONGOS_URI);
const donorConn = new Mongo(process.env.SHARD0_URI);
const recipientConn = new Mongo(process.env.SHARD1_URI);

const viaMongos = () => mongosConn.getDB(DB_NAME).getCollection(COLL_NAME);
const onDonor = () => donorConn.getDB(DB_NAME).getCollection(COLL_NAME);
const onRecipient = () => recipientConn.getDB(DB_NAME).getCollection(COLL_NAME);

// ---------------------------------------------------------------- printing

function clock(d = new Date()) {
  // HH:MM:SS.mmm in local time, the same clock the server log lines use
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}
function say(msg) {
  print(`[${clock()}] ${msg}`);
}
function heading(title) {
  print("\n" + "=".repeat(78) + "\n" + title + "\n" + "=".repeat(78));
}
function check(label, ok) {
  print(`  ${ok ? "PASS" : "FAIL"}  ${label}`);
  if (!ok) globalThis.FAILURES = (globalThis.FAILURES || 0) + 1;
  return ok;
}
function finish() {
  const n = globalThis.FAILURES || 0;
  print(n === 0 ? "\nAll checks passed." : `\n${n} check(s) FAILED.`);
  if (n) quit(1);
}

// ---------------------------------------------------------------- failpoints
// Migration steps, numbered exactly as in the server's failpoint names.
// Donor (src/mongo/db/s/migration_source_manager.cpp): moveChunkHangAtStep<N>
const DONOR_STEPS = {
  1: "parsed options, about to start",
  2: "installed migration source manager",
  3: "started clone (recipient told to begin)",
  4: "recipient caught up, about to enter critical section",
  5: "inside critical section, recipient has committed data",
  6: "migration committed and cleaned up",
};
// Recipient (src/mongo/db/s/migration_destination_manager.cpp): migrateThreadHangAtStep<N>
const RECIPIENT_STEPS = {
  1: "deleted any prior data in range",
  2: "copied indexes",
  3: "scheduled range deletion task",
  4: "finished cloning documents",
  5: "finished first catch-up, about to enter steady state",
  6: "steady state ended: donor asked it to commit",
  7: "finished final catch-up, about to enter its own critical section",
};

// What each step's duration measures in the config.changelog timings
// ("step N of 6" for the donor, "step N of 8" for the recipient). Each number is
// the milliseconds spent getting from step N-1 to step N.
const DONOR_PHASES = {
  1: "check request, register migration",
  2: "set up migration source manager",
  3: "start cloner, tell recipient to begin",
  4: "clone + catch-up (waiting for recipient)",
  5: "critical section: final catch-up, recipient commits data",
  6: "commit on config server, leave critical section",
};
const RECIPIENT_PHASES = {
  1: "wait for any earlier deletion of this range",
  2: "copy indexes",
  3: "record range-deletion task (cleanup if migration fails)",
  4: "clone documents",
  5: "first catch-up of writes made during the clone",
  6: "steady state: keep catching up until donor commits",
  7: "final catch-up after donor's commit request",
  8: "own critical section, wait for decision, finish",
};

function setFailPoint(conn, name, on) {
  const res = conn.getDB("admin").runCommand({configureFailPoint: name, mode: on ? "alwaysOn" : "off"});
  if (!res.ok) throw new Error(`configureFailPoint ${name} failed: ${EJSON.stringify(res)}`);
}
function pauseDonorAt(step) {
  setFailPoint(donorConn, "moveChunkHangAtStep" + step, true);
  say(`Donor will pause at step ${step}: ${DONOR_STEPS[step]}  (failpoint moveChunkHangAtStep${step})`);
}
function resumeDonorAt(step) {
  setFailPoint(donorConn, "moveChunkHangAtStep" + step, false);
  say(`Donor released from step ${step}`);
}
function pauseRecipientAt(step) {
  setFailPoint(recipientConn, "migrateThreadHangAtStep" + step, true);
  say(`Recipient will pause at step ${step}: ${RECIPIENT_STEPS[step]}  (failpoint migrateThreadHangAtStep${step})`);
}
function resumeRecipientAt(step) {
  setFailPoint(recipientConn, "migrateThreadHangAtStep" + step, false);
  say(`Recipient released from step ${step}`);
}
function clearAllFailPoints() {
  const names = [];
  for (let i = 1; i <= 6; i++) names.push("moveChunkHangAtStep" + i);
  for (let i = 1; i <= 7; i++) names.push("migrateThreadHangAtStep" + i);
  names.push("hangBeforeLeavingCriticalSection", "hangBeforeEnteringCriticalSection", "hangBeforePostMigrationCommitRefresh");
  for (const c of [donorConn, recipientConn])
    for (const n of names) c.getDB("admin").runCommand({configureFailPoint: n, mode: "off"});
}

// Wait until the donor's migration reports it is at the given step.
// The donor's operation shows up in $currentOp as desc "MoveChunk", msg "step N of 6".
function waitForDonorStep(step, timeoutMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const ops = donorConn.getDB("admin")
      .aggregate([{$currentOp: {allUsers: true, idleConnections: true}}, {$match: {desc: "MoveChunk"}}])
      .toArray();
    if (ops.some(op => op.msg && op.msg.startsWith("step " + step))) {
      say(`Donor is paused at step ${step}`);
      return;
    }
    sleep(100);
  }
  throw new Error(`donor never reached step ${step}`);
}
// The recipient's thread shows up as desc "migrateThread", msg "step N of 7".
function waitForRecipientStep(step, timeoutMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const ops = recipientConn.getDB("admin").currentOp(true).inprog.filter(op => op.desc === "migrateThread");
    if (ops.some(op => op.msg && op.msg.startsWith("step " + step))) {
      say(`Recipient is paused at step ${step}`);
      return;
    }
    sleep(100);
  }
  throw new Error(`recipient never reached step ${step}`);
}

// ---------------------------------------------------------------- background work
// A paused migration blocks whoever started it, so the migration (and any write we
// expect to block) runs in a separate mongosh process. Its JSON result is read back
// when it exits.

function runInBackground(label, js) {
  const cp = require("child_process");
  const fs = require("fs");
  const outFile = `${process.env.DATA_DIR}/bg-${label}-${Date.now()}.json`;
  const script = `
    const t0 = Date.now();
    let res;
    try { res = (function () { ${js} })(); } catch (e) { res = {ok: 0, errmsg: String(e)}; }
    require("fs").writeFileSync(${JSON.stringify(outFile)},
      EJSON.stringify({res, elapsedMs: Date.now() - t0}, {relaxed: true}));
  `;
  const child = cp.spawn("mongosh", [process.env.MONGOS_URI, "--quiet", "--norc", "--eval", script],
                         {stdio: "ignore"});
  return {label, child, outFile, startedAt: Date.now()};
}
function isDone(h) {
  return h.child.exitCode !== null;
}
function waitFor(h, timeoutMs = 120000) {
  while (!isDone(h)) {
    if (Date.now() - h.startedAt > timeoutMs) throw new Error(`${h.label} did not finish in ${timeoutMs} ms`);
    sleep(100);
  }
  const fs = require("fs");
  if (!fs.existsSync(h.outFile)) throw new Error(`${h.label} exited without a result (code ${h.child.exitCode})`);
  const out = EJSON.parse(fs.readFileSync(h.outFile, "utf8"));
  fs.unlinkSync(h.outFile);
  return out;
}

function startMigration() {
  say(`Starting moveRange: ${NS} [k: ${SPLIT}, MaxKey) from ${DONOR} to ${RECIPIENT}`);
  return runInBackground("moveRange", `
    return db.adminCommand({moveRange: "${NS}", min: {k: ${SPLIT}}, toShard: "${RECIPIENT}"});
  `);
}
function waitForMigration(h) {
  const out = waitFor(h);
  say(`moveRange returned ok=${out.res.ok} after ${out.elapsedMs} ms` +
      (out.res.ok ? "" : `: ${out.res.codeName || ""} ${out.res.errmsg || ""}`));
  return out;
}

// ---------------------------------------------------------------- inspecting state

// Most recent config.changelog entry of a given kind since a time, e.g. "moveChunk.commit".
function lastChangelog(what, since) {
  return mongosConn.getDB("config").changelog
    .find({ns: NS, what, time: {$gte: since}}).sort({time: -1}).limit(1).toArray()[0];
}
// Which shard owns each chunk, according to the config server.
function chunkOwners() {
  const cfg = mongosConn.getDB("config");
  const uuid = cfg.collections.findOne({_id: NS}).uuid;
  return cfg.chunks.find({uuid}).sort({min: 1}).toArray()
    .map(c => `[${fmtKey(c.min.k)}, ${fmtKey(c.max.k)}) on ${c.shard}`);
}
function fmtKey(v) {
  if (v instanceof MinKey || (v && v._bsontype === "MinKey")) return "MinKey";
  if (v instanceof MaxKey || (v && v._bsontype === "MaxKey")) return "MaxKey";
  return String(v);
}
function printOwners() {
  for (const line of chunkOwners()) print("  chunk " + line);
}

// Documents in the moving range, counted three ways. Direct shard queries do not
// filter orphans, so they show what is physically stored on each shard.
function rangeCounts() {
  const q = {k: {$gte: SPLIT}};
  return {
    donor: onDonor().countDocuments(q),
    recipient: onRecipient().countDocuments(q),
    mongos: viaMongos().countDocuments(q),
  };
}
function printCounts(label) {
  const c = rangeCounts();
  print(`  ${label}: docs with k >= ${SPLIT}  stored on donor=${c.donor}  stored on recipient=${c.recipient}  seen via mongos=${c.mongos}`);
  return c;
}

// One document's state on the donor, on the recipient and through mongos.
function docState(k) {
  const show = d => (d ? `v=${d.v}` : "missing");
  return {
    donor: show(onDonor().findOne({k})),
    recipient: show(onRecipient().findOne({k})),
    mongos: show(viaMongos().findOne({k})),
  };
}
function printDoc(k, label = "") {
  const s = docState(k);
  print(`  k=${k}${label ? " (" + label + ")" : ""}:  donor ${s.donor} | recipient ${s.recipient} | mongos ${s.mongos}`);
  return s;
}

// Migration-related lines from a node's in-memory log (last ~1000 lines).
// Log components: MIGRATE = migration code, SHARDING = general sharding, RDELETER = range deleter.
const MIGRATION_COMPONENTS = ["MIGRATE", "SHARDING"];
function migrationLogs(conn, since, components = MIGRATION_COMPONENTS) {
  const lines = conn.getDB("admin").runCommand({getLog: "global"}).log;
  const out = [];
  for (const line of lines) {
    let e;
    try { e = JSON.parse(line); } catch (_) { continue; }
    const t = new Date(e.t.$date);
    if (t < since) continue;
    if (!components.includes(e.c)) continue;
    if (e.c !== "RDELETER" && !/igrat|ritical section|range|chunk|ransfer|clone|catch/i.test(e.msg)) continue;
    out.push(e);
  }
  return out;
}
function printLogs(title, conn, since, components = MIGRATION_COMPONENTS) {
  print(`\n  -- ${title} --`);
  const entries = migrationLogs(conn, since, components);
  if (!entries.length) print("  (no matching log lines)");
  for (const e of entries) {
    // A few attributes say what was decided or what went wrong; show those.
    const a = e.attr || {};
    const extra = ["decision", "error", "reason"].filter(k => a[k] !== undefined)
      .map(k => `${k}=${typeof a[k] === "object" ? (a[k].codeName || a[k].errmsg || JSON.stringify(a[k])) : a[k]}`);
    print(`  ${clock(new Date(e.t.$date))}  id=${String(e.id).padEnd(8)} ${e.c.padEnd(9)} ${e.msg}` +
          (extra.length ? `  [${extra.join(", ").slice(0, 120)}]` : ""));
  }
}

// ---------------------------------------------------------------- data setup

// Drop and recreate test.users with DOCS documents, split into two chunks at SPLIT,
// both chunks on shard0. Every experiment starts from this state.
function resetData() {
  clearAllFailPoints();
  const db = mongosConn.getDB(DB_NAME);
  db.dropDatabase();
  let res = mongosConn.getDB("admin").runCommand({enableSharding: DB_NAME, primaryShard: DONOR});
  if (!res.ok) throw new Error("enableSharding failed: " + EJSON.stringify(res));
  res = mongosConn.getDB("admin").runCommand({shardCollection: NS, key: {k: 1}});
  if (!res.ok) throw new Error("shardCollection failed: " + EJSON.stringify(res));

  const pad = "x".repeat(200);
  for (let start = 0; start < DOCS; start += 10000) {
    const batch = [];
    for (let k = start; k < Math.min(start + 10000, DOCS); k++) batch.push({k, v: 0, pad});
    db.getCollection(COLL_NAME).insertMany(batch, {ordered: false});
  }
  res = mongosConn.getDB("admin").runCommand({split: NS, middle: {k: SPLIT}});
  if (!res.ok) throw new Error("split failed: " + EJSON.stringify(res));
  // Query both chunks once so mongos caches the current routing, as a busy router would.
  viaMongos().findOne({k: 0});
  viaMongos().findOne({k: SPLIT});
  say(`Loaded ${DOCS} documents into ${NS}, split at k=${SPLIT}, all on ${DONOR}`);
}

// Run a command through mongos with a time limit. Returns what happened instead of
// throwing, so "it waited until the limit" can be reported as a result.
function timedCommand(cmd, maxTimeMS = 3000) {
  const t = Date.now();
  let res, code = 0;
  try {
    res = mongosConn.getDB(DB_NAME).runCommand(Object.assign({}, cmd, {maxTimeMS}));
    if (!res.ok) code = res.code;
    else if (res.writeErrors && res.writeErrors.length) code = res.writeErrors[0].code;
  } catch (e) {
    code = e.code || -1;
  }
  const ms = Date.now() - t;
  return {res, code, ms, timedOut: code === 50, text: code === 0 ? `succeeded in ${ms} ms` : code === 50 ? `still waiting at the ${maxTimeMS} ms limit (MaxTimeMSExpired)` : `error ${code} after ${ms} ms`};
}

// ---------------------------------------------------------------- killing and restarting nodes
// Node names match the data folders and pid files: config-0, shard0-0, shard0-1, ...

const SET_URI = {config: process.env.CONFIG_URI, shard0: process.env.SHARD0_URI, shard1: process.env.SHARD1_URI};
const SET_BASE_PORT = {config: Number(process.env.CONFIG_PORT), shard0: Number(process.env.SHARD0_PORT),
                       shard1: Number(process.env.SHARD1_PORT)};
const PROJECT_DIR = process.env.DEMO_JS_DIR + "/..";

// Name of the member that is currently primary, e.g. "shard0-1", or null if none.
function primaryOf(set) {
  try {
    const c = new Mongo(SET_URI[set] + "&serverSelectionTimeoutMS=2000");
    const me = c.getDB("admin").runCommand({hello: 1}).me;
    c.close();
    return `${set}-${Number(me.split(":")[1]) - SET_BASE_PORT[set]}`;
  } catch (e) {
    return null;
  }
}

// SIGKILL a node: the process stops instantly, mid-operation, like a power cut.
function killNode(name) {
  const fs = require("fs");
  const pid = Number(fs.readFileSync(`${process.env.DATA_DIR}/${name}.pid`, "utf8").trim());
  process.kill(pid, "SIGKILL");
  const t = Date.now();
  while (Date.now() - t < 10000) {
    try { process.kill(pid, 0); } catch (e) { break; } // signal 0 = "does it still exist?"
    sleep(50);
  }
  say(`KILLED ${name} (pid ${pid}) with SIGKILL`);
  return {name, pid, at: new Date()};
}
function killPrimary(set) {
  const name = primaryOf(set);
  if (!name) throw new Error(`${set} has no primary to kill`);
  return killNode(name);
}

// Wait until some other member of the set has been elected primary.
function waitForNewPrimary(set, oldName, timeoutMs = 90000) {
  const t = Date.now();
  while (Date.now() - t < timeoutMs) {
    const p = primaryOf(set);
    if (p && p !== oldName) {
      say(`${set} elected a new primary: ${p} (${Date.now() - t} ms after the kill)`);
      return p;
    }
    sleep(250);
  }
  throw new Error(`${set} elected no new primary within ${timeoutMs} ms`);
}

// Start a killed node again with its original options (same script as ./restart-node.sh).
function restartNode(name) {
  try {
    require("child_process").execFileSync("bash", [PROJECT_DIR + "/restart-node.sh", name], {stdio: "pipe"});
  } catch (e) {
    throw new Error(`could not restart ${name}: ${String(e.stderr || e.stdout || e.message).trim()}`);
  }
  say(`Restarted ${name}`);
}

// Wait until every member of a set is up as primary or secondary.
function waitForSetHealthy(set, timeoutMs = 120000) {
  const t = Date.now();
  while (Date.now() - t < timeoutMs) {
    try {
      const c = new Mongo(SET_URI[set] + "&serverSelectionTimeoutMS=2000");
      const st = c.getDB("admin").runCommand({replSetGetStatus: 1});
      c.close();
      if (st.ok && st.members.every(m => m.health === 1 && (m.state === 1 || m.state === 2))) {
        say(`${set}: all ${st.members.length} members healthy again`);
        return;
      }
    } catch (e) { /* set not reachable yet */ }
    sleep(500);
  }
  throw new Error(`${set} not healthy within ${timeoutMs} ms`);
}

// ---------------------------------------------------------------- recovery records
// The donor keeps a durable note of an unfinished migration in config.migrationCoordinators,
// and the recipient one in config.migrationRecipients once it holds its critical section.
// A new primary reads these to finish the migration. Each is deleted once it is resolved.

function coordinatorDocs() {
  return donorConn.getDB("config").getCollection("migrationCoordinators").find().toArray();
}
function recipientRecoveryDocs() {
  return recipientConn.getDB("config").getCollection("migrationRecipients").find().toArray();
}
function printRecoveryRecords(label) {
  const c = coordinatorDocs();
  const r = recipientRecoveryDocs();
  print(`  ${label}:`);
  print(`    donor config.migrationCoordinators: ` +
        (c.length ? c.map(d => `{migrationId ${d._id.toString("hex").slice(0, 8)}…, range [${fmtKey(d.range.min.k)}, ${fmtKey(d.range.max.k)}), decision: ${d.decision || "(none yet)"}}`).join(", ") : "empty"));
  print(`    recipient config.migrationRecipients: ` + (r.length ? `${r.length} document(s): recipient critical section is held` : "empty"));
}
// Recovery is finished when both notes are gone.
function waitForRecoveryRecordsCleared(timeoutMs = 180000) {
  const t = Date.now();
  while (Date.now() - t < timeoutMs) {
    try {
      if (coordinatorDocs().length === 0 && recipientRecoveryDocs().length === 0) {
        say(`Recovery records cleared (${Date.now() - t} ms): the migration's outcome is settled on both shards`);
        return;
      }
    } catch (e) { /* primary changing; try again */ }
    sleep(250);
  }
  throw new Error("recovery records still present after " + timeoutMs + " ms");
}

// ---------------------------------------------------------------- workload and checker
// A separate mongosh process writes continuously through mongos with majority write
// concern and retryable writes (the driver retries once after a primary change). Every
// write is logged, with its outcome, to a file the checker reads afterwards.
//   ins-moving: insert a new doc in the moving chunk      {k: WL_BASE + seq, wl: seq}
//   ins-other : insert a new doc in the chunk that stays  {k: -seq, wl: seq}
//   inc       : increment one counter doc in the moving chunk

const WL_BASE = DOCS + 100000;
const WL_COUNTER_K = SPLIT + 40;

function startWorkload() {
  const logFile = `${process.env.DATA_DIR}/workload-${Date.now()}.jsonl`;
  const stopFile = logFile + ".stop";
  const script = `
    const fs = require("fs");
    const coll = db.getSiblingDB("${DB_NAME}").getCollection("${COLL_NAME}");
    const wc = {writeConcern: {w: "majority"}};
    let seq = 0;
    while (!fs.existsSync(${JSON.stringify(stopFile)})) {
      seq++;
      const op = ["ins-moving", "ins-other", "inc"][seq % 3];
      const rec = {seq, op, t: Date.now()};
      try {
        if (op === "ins-moving") coll.insertOne({k: ${WL_BASE} + seq, wl: seq}, wc);
        else if (op === "ins-other") coll.insertOne({k: -seq, wl: seq}, wc);
        else coll.updateOne({k: ${WL_COUNTER_K}}, {$inc: {n: 1}}, wc);
        rec.ok = 1;
      } catch (e) {
        rec.ok = 0;
        rec.err = String(e.codeName || e.message).slice(0, 100);
      }
      rec.ms = Date.now() - rec.t;
      fs.appendFileSync(${JSON.stringify(logFile)}, JSON.stringify(rec) + "\\n");
      sleep(5);
    }
  `;
  const child = require("child_process").spawn(
    "mongosh", [process.env.MONGOS_URI, "--quiet", "--norc", "--eval", script], {stdio: "ignore"});
  say("Workload started: continuous majority-acknowledged writes to both chunks through mongos");
  return {child, logFile, stopFile, startedAt: Date.now()};
}

function stopWorkload(w) {
  const fs = require("fs");
  fs.writeFileSync(w.stopFile, "");
  const t = Date.now();
  while (w.child.exitCode === null && Date.now() - t < 120000) sleep(100);
  const recs = fs.existsSync(w.logFile)
    ? fs.readFileSync(w.logFile, "utf8").trim().split("\n").filter(Boolean).map(l => JSON.parse(l))
    : [];
  fs.rmSync(w.stopFile, {force: true});
  say(`Workload stopped after ${recs.length} writes`);
  return recs;
}

// What clients experienced: how many writes, failures, and the longest stall.
function summarizeWorkload(recs, events = []) {
  const ok = recs.filter(r => r.ok).length;
  const failed = recs.filter(r => !r.ok);
  const slowest = recs.reduce((a, r) => (r.ms > a.ms ? r : a), {ms: 0});
  print(`  writes attempted: ${recs.length}   acknowledged: ${ok}   returned an error: ${failed.length}`);
  if (failed.length) {
    const kinds = {};
    for (const r of failed) kinds[r.err] = (kinds[r.err] || 0) + 1;
    print(`  errors: ` + Object.entries(kinds).map(([k, n]) => `${k} x${n}`).join(", "));
  }
  if (slowest.seq)
    print(`  slowest write: ${slowest.ms} ms (${slowest.op}, started ${clock(new Date(slowest.t))})`);
  // A per-second view of completed writes around the interesting moments.
  if (recs.length) {
    const start = Math.floor(recs[0].t / 1000), end = Math.floor((recs[recs.length - 1].t + recs[recs.length - 1].ms) / 1000);
    const perSec = new Array(end - start + 1).fill(0);
    for (const r of recs) if (r.ok) perSec[Math.floor((r.t + r.ms) / 1000) - start]++;
    const marks = events.map(e => [Math.floor(e.at.getTime() / 1000) - start, e.label]);
    let line = "";
    for (let i = 0; i < perSec.length; i++) {
      const m = marks.find(x => x[0] === i);
      line += (m ? `|${m[1]}| ` : "") + perSec[i] + " ";
    }
    print(`  writes completed per second (from ${clock(new Date(start * 1000))}):`);
    print("    " + line.trim());
  }
}

// The checker: every acknowledged write must be present exactly once, and nothing applied twice.
function checkWorkload(recs) {
  const docs = mongosConn.getDB(DB_NAME).getCollection(COLL_NAME)
    .find({wl: {$exists: true}}, {wl: 1, _id: 0}).readConcern("majority").toArray();
  const copies = new Map();
  for (const d of docs) copies.set(d.wl, (copies.get(d.wl) || 0) + 1);

  const inserts = recs.filter(r => r.op !== "inc");
  const ackedIns = inserts.filter(r => r.ok);
  const failedIns = inserts.filter(r => !r.ok);
  const missing = ackedIns.filter(r => !copies.has(r.seq));
  const duplicated = [...copies.entries()].filter(([, n]) => n > 1);
  const failedButPresent = failedIns.filter(r => copies.has(r.seq)).length;

  const incs = recs.filter(r => r.op === "inc");
  const ackedInc = incs.filter(r => r.ok).length;
  const failedInc = incs.length - ackedInc;
  const counterDoc = viaMongos().findOne({k: WL_COUNTER_K});
  const n = (counterDoc && counterDoc.n) || 0;

  print(`  inserts acknowledged: ${ackedIns.length}, found: ${ackedIns.length - missing.length}, missing: ${missing.length}, stored twice: ${duplicated.length}`);
  if (failedIns.length)
    print(`  inserts that returned an error: ${failedIns.length} (outcome unknown to the client; ${failedButPresent} were in fact applied)`);
  print(`  counter: ${ackedInc} acknowledged increments, ${failedInc} returned an error; stored value n=${n}`);
  check("every acknowledged insert is present", missing.length === 0);
  check("no insert was stored twice", duplicated.length === 0);
  check("counter includes every acknowledged increment and none twice " +
        `(${ackedInc} <= n <= ${ackedInc + failedInc})`, n >= ackedInc && n <= ackedInc + failedInc);
}

// After the outcome is settled, the shard that does NOT own the range should end up storing
// none of it: the recipient deletes a partial copy after an abort, the donor its orphans after
// a commit. (Orphan deletion waits orphanCleanupDelaySecs first.)
function waitForNonOwnerEmpty(timeoutMs) {
  const owner = chunkOwners().find(l => l.startsWith(`[${SPLIT},`)).split(" on ")[1];
  const nonOwnerConn = owner === DONOR ? recipientConn : donorConn;
  const nonOwner = owner === DONOR ? RECIPIENT : DONOR;
  const limit = timeoutMs || (Number(process.env.ORPHAN_DELAY_SECS) + 120) * 1000;
  const t = Date.now();
  let last = -1;
  while (Date.now() - t < limit) {
    let n;
    try { n = nonOwnerConn.getDB(DB_NAME).getCollection(COLL_NAME).countDocuments({k: {$gte: SPLIT}}); }
    catch (e) { sleep(500); continue; }
    if (n !== last) { say(`${nonOwner} (not the owner) still stores ${n} documents from the range`); last = n; }
    if (n === 0) return {owner, nonOwner, empty: true};
    sleep(1000);
  }
  return {owner, nonOwner, empty: false};
}
