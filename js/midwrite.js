// Mid-migration writes: pause the migration at a chosen phase with a failpoint, write
// to the moving range through mongos, look at where the write landed, then let the
// migration finish and check that nothing was lost, duplicated or left behind.
//
// Run one case with CASE=<name>, or all of them with CASE=all (the default):
//   clone     recipient paused after cloning, before catch-up
//   catchup   donor paused with recipient in steady state, before the critical section
//   critsec   donor paused inside the critical section (writes block, reads still allowed)
//   commit    donor paused right after the config commit, still in the critical section (reads block too)
//   after     no pause: writes right after the commit, while the donor still holds orphans
//   retry     retryable writes made before the migration, retried after it
load(process.env.DEMO_JS_DIR + "/lib.js");

// Keys we write to. All are inside the moving range [SPLIT, MaxKey).
const K_UPDATE = SPLIT + 10;     // existing doc we update
const K_DELETE = SPLIT + 20;     // existing doc we delete
const K_INSERT = DOCS + 5;       // new doc we insert
const K_TIMEOUT = SPLIT + 11;    // write that times out (critsec case)
const K_OTHER = 5;               // doc in the chunk that is NOT moving (stays on the donor)
const K_RETRY_INC = SPLIT + 30;  // retryable findAndModify (retry case)
const K_RETRY_INS = DOCS + 7;    // retryable insert (retry case)

const testDB = () => mongosConn.getDB(DB_NAME);

// Insert, update and delete through mongos, each timed.
function writeThree(tag) {
  const coll = viaMongos();
  let t = Date.now();
  coll.insertOne({k: K_INSERT, v: tag, pad: "new"});
  say(`  insert k=${K_INSERT} v=${tag}            ${Date.now() - t} ms`);
  t = Date.now();
  coll.updateOne({k: K_UPDATE}, {$set: {v: tag}});
  say(`  update k=${K_UPDATE} set v=${tag}         ${Date.now() - t} ms`);
  t = Date.now();
  coll.deleteOne({k: K_DELETE});
  say(`  delete k=${K_DELETE}                  ${Date.now() - t} ms`);
}
function showThree() {
  printDoc(K_INSERT, "inserted");
  printDoc(K_UPDATE, "updated");
  printDoc(K_DELETE, "deleted");
}
// After the migration: the recipient owns the range and must reflect all three writes.
function verifyThree(tag) {
  const s = {ins: docState(K_INSERT), upd: docState(K_UPDATE), del: docState(K_DELETE)};
  check(`inserted doc visible via mongos and stored on recipient (v=${tag})`,
        s.ins.mongos === `v=${tag}` && s.ins.recipient === `v=${tag}`);
  check(`updated doc has new value via mongos and on recipient (v=${tag})`,
        s.upd.mongos === `v=${tag}` && s.upd.recipient === `v=${tag}`);
  check("deleted doc gone via mongos and on recipient",
        s.del.mongos === "missing" && s.del.recipient === "missing");
  const c = rangeCounts();
  check(`recipient stores exactly ${SPLIT} docs in the range (+1 insert, -1 delete)`, c.recipient === SPLIT);
  check("mongos sees the same count (no duplicates, nothing lost)", c.mongos === SPLIT);
}
function ownerOfRange() {
  return chunkOwners().find(l => l.startsWith(`[${SPLIT},`)).split(" on ")[1];
}

// ------------------------------------------------------------------------------------

const CASES = {};

CASES.clone = function () {
  heading("CASE clone: write after the recipient has cloned, before it catches up");
  print("Expected: writes go to the donor (still the owner) and succeed immediately. The recipient's");
  print("copy is stale until it resumes and pulls the donor's recorded changes (the catch-up phase).\n");
  resetData();
  pauseRecipientAt(4);
  const h = startMigration();
  waitForRecipientStep(4);
  say("Writing through mongos while the recipient is paused:");
  writeThree(1);
  say("Where each write is stored right now:");
  showThree();
  const mid = docState(K_UPDATE);
  check("donor has the update", mid.donor === "v=1");
  check("recipient's cloned copy is still the old value", mid.recipient === "v=0");
  resumeRecipientAt(4);
  check("migration succeeded", waitForMigration(h).res.ok === 1);
  verifyThree(1);
};

CASES.catchup = function () {
  heading("CASE catchup: write while the recipient is in steady state, before the critical section");
  print("Expected: writes go to the donor and succeed immediately. The recipient keeps polling the");
  print("donor for changes even though the donor is paused, so it picks them up within moments.\n");
  resetData();
  pauseDonorAt(4);
  const h = startMigration();
  waitForDonorStep(4);
  say("Writing through mongos while the donor is paused:");
  writeThree(2);
  say("Immediately after the writes:");
  showThree();
  say("Waiting up to 5 s to see whether the recipient picks them up while the donor is still paused...");
  const t = Date.now();
  while (Date.now() - t < 5000 && docState(K_UPDATE).recipient !== "v=2") sleep(100);
  const s = printDoc(K_UPDATE, "updated");
  check("recipient applied the update while the donor was still paused (steady-state catch-up)", s.recipient === "v=2");
  resumeDonorAt(4);
  check("migration succeeded", waitForMigration(h).res.ok === 1);
  verifyThree(2);
};

CASES.critsec = function () {
  heading("CASE critsec: write while the donor is inside the critical section");
  print("Expected: reads still succeed, but writes wait. A write with a time limit fails without");
  print("being applied; a write without one waits, then lands on the recipient. The critical section");
  print("covers the whole collection on the donor, so writes to the chunk that is NOT moving wait too.\n");
  resetData();
  pauseDonorAt(5);
  const h = startMigration();
  waitForDonorStep(5);

  let r = timedCommand({find: COLL_NAME, filter: {k: K_UPDATE}});
  say(`read   k=${K_UPDATE} (moving chunk), 3 s limit: ${r.text}`);
  check("reads of the moving chunk are not blocked in this part of the critical section", r.code === 0);

  r = timedCommand({update: COLL_NAME, updates: [{q: {k: K_TIMEOUT}, u: {$set: {v: 99}}}]});
  say(`update k=${K_TIMEOUT} (moving chunk), 3 s limit: ${r.text}`);
  check("write to the moving chunk waited until its time limit", r.timedOut);

  r = timedCommand({update: COLL_NAME, updates: [{q: {k: K_OTHER}, u: {$set: {v: 99}}}]});
  say(`update k=${K_OTHER} (chunk NOT moving), 3 s limit: ${r.text}`);
  check("write to the chunk that is not moving waited too (critical section is collection-wide)", r.timedOut);

  say(`Starting update k=${K_UPDATE} set v=3 with no time limit, in the background...`);
  const bw = runInBackground("blockedWrite",
    `return db.getSiblingDB("${DB_NAME}").runCommand({update: "${COLL_NAME}", updates: [{q: {k: ${K_UPDATE}}, u: {$set: {v: 3}}}]});`);
  sleep(4000);
  check("after 4 s the background write is still waiting", !isDone(bw));
  printDoc(K_UPDATE, "while write is waiting");

  resumeDonorAt(5);
  check("migration succeeded", waitForMigration(h).res.ok === 1);
  const o = waitFor(bw);
  say(`background write finished: nModified=${o.res.nModified} after ${o.elapsedMs} ms`);
  check("background write succeeded once the critical section ended", o.res.ok === 1 && o.res.nModified === 1);
  const s = printDoc(K_UPDATE, "after");
  check("the write landed on the recipient (new owner), not the donor", s.recipient === "v=3" && s.donor === "v=0");
  const s2 = printDoc(K_TIMEOUT, "timed-out write");
  check("the timed-out write was not applied anywhere", s2.recipient === "v=0" && s2.mongos === "v=0");
  const s3 = printDoc(K_OTHER, "timed-out write, other chunk");
  check("the timed-out write to the other chunk was not applied", s3.donor === "v=0");
};

CASES.commit = function () {
  heading("CASE commit: reads right after the config server commits, before the donor refreshes");
  print("Expected: the config server already lists the recipient as owner, but the donor's critical");
  print("section is in its commit phase, so reads wait as well as writes, for both chunks.\n");
  resetData();
  setFailPoint(donorConn, "hangBeforePostMigrationCommitRefresh", true);
  say("Donor will pause after the config commit, before refreshing its metadata (failpoint hangBeforePostMigrationCommitRefresh)");
  const h = startMigration();
  const t0 = Date.now();
  while (ownerOfRange() !== RECIPIENT) {
    if (Date.now() - t0 > 60000) throw new Error("commit never happened");
    sleep(100);
  }
  sleep(500);
  say("Config server now records the new owner:");
  printOwners();
  check("migration has not returned yet (donor still paused)", !isDone(h));

  let r = timedCommand({find: COLL_NAME, filter: {k: K_UPDATE}});
  say(`read k=${K_UPDATE} (moved chunk), 3 s limit: ${r.text}`);
  check("read of the moved chunk waited (mongos still routes it to the donor)", r.timedOut);
  r = timedCommand({find: COLL_NAME, filter: {k: K_OTHER}});
  say(`read k=${K_OTHER} (chunk NOT moving), 3 s limit: ${r.text}`);
  check("read of the chunk that stays on the donor waited too", r.timedOut);

  say(`Starting read k=${K_UPDATE} with no time limit, in the background...`);
  const br = runInBackground("blockedRead",
    `return db.getSiblingDB("${DB_NAME}").runCommand({find: "${COLL_NAME}", filter: {k: ${K_UPDATE}}});`);
  sleep(3000);
  check("after 3 s the background read is still waiting", !isDone(br));

  setFailPoint(donorConn, "hangBeforePostMigrationCommitRefresh", false);
  say("Donor released");
  check("migration succeeded", waitForMigration(h).res.ok === 1);
  const o = waitFor(br);
  const n = o.res.cursor ? o.res.cursor.firstBatch.length : 0;
  say(`background read finished: ${n} document(s) after ${o.elapsedMs} ms`);
  check("background read returned the document once the critical section ended", o.res.ok === 1 && n === 1);
};

CASES.after = function () {
  heading("CASE after: writes right after the commit, while the donor still stores orphans");
  print("Expected: new writes go only to the recipient. The donor's old copy is stale but invisible");
  print("through mongos, and is deleted after orphanCleanupDelaySecs.\n");
  resetData();
  const h = startMigration();
  check("migration succeeded", waitForMigration(h).res.ok === 1);
  printCounts("just after commit");
  say("Writing through mongos:");
  writeThree(4);
  showThree();
  const ins = docState(K_INSERT), upd = docState(K_UPDATE), del = docState(K_DELETE);
  check("insert stored only on the recipient", ins.recipient === "v=4" && ins.donor === "missing");
  check("donor's orphan copy is stale (v=0) but mongos shows the new value", upd.donor === "v=0" && upd.mongos === "v=4");
  check("deleted doc still exists as an orphan on the donor, but not via mongos", del.donor === "v=0" && del.mongos === "missing");
  verifyThree(4);
};

CASES.retry = function () {
  heading("CASE retry: retryable writes made before the migration, retried after it");
  print("Expected: the retries are recognized as already done, even though they now go to a different");
  print("shard, because the donor transfers its record of executed writes along with the documents.\n");
  resetData();
  // A retryable write is identified by (session id, txnNumber), and a session only remembers its
  // latest txnNumber, so each write gets its own session. A driver session supplies the session
  // id; its server session may be reused from the driver's pool, so txnNumbers start from the
  // current time to be newer than any earlier use.
  const incDB = mongosConn.startSession({retryWrites: false}).getDatabase(DB_NAME);
  const insDB = mongosConn.startSession({retryWrites: false}).getDatabase(DB_NAME);
  const base = Date.now();
  const inc = {findAndModify: COLL_NAME, query: {k: K_RETRY_INC}, update: {$inc: {v: 1}}, new: true,
               txnNumber: NumberLong(String(base))};
  const ins = {insert: COLL_NAME, documents: [{k: K_RETRY_INS, v: 1}], txnNumber: NumberLong(String(base))};

  pauseDonorAt(4);
  const h = startMigration();
  waitForDonorStep(4);
  const r1 = incDB.runCommand(inc);
  say(`findAndModify $inc k=${K_RETRY_INC} (session A, txnNumber N): new value v=${r1.value.v}`);
  const r2 = insDB.runCommand(ins);
  say(`insert k=${K_RETRY_INS} (session B, txnNumber N): n=${r2.n}`);
  resumeDonorAt(4);
  check("migration succeeded", waitForMigration(h).res.ok === 1);
  say(`Range now owned by ${ownerOfRange()}. Retrying both commands with the same session and txnNumber:`);

  const r1b = incDB.runCommand(inc);
  say(`retry findAndModify: returned v=${r1b.value.v}`);
  const r2b = insDB.runCommand(ins);
  say(`retry insert: ok=${r2b.ok} n=${r2b.n}`);
  check("retried findAndModify returned the original result (v=1), not a second increment", r1b.value.v === 1);
  check("the document was incremented once (v=1)", viaMongos().findOne({k: K_RETRY_INC}).v === 1);
  check("retried insert reported success", r2b.ok === 1 && r2b.n === 1);
  check("only one copy of the inserted document exists", viaMongos().countDocuments({k: K_RETRY_INS}) === 1);

  const fresh = incDB.runCommand(Object.assign({}, inc, {txnNumber: NumberLong(String(base + 1))}));
  check("a new txnNumber does increment again (v=2), so the retry check is real", fresh.value.v === 2);
};

// ------------------------------------------------------------------------------------

const which = (process.env.CASE || "all").toLowerCase();
const order = ["clone", "catchup", "critsec", "commit", "after", "retry"];
const toRun = which === "all" ? order : which.split(",");
for (const name of toRun) {
  if (!CASES[name]) { print(`Unknown case "${name}". Choose from: ${order.join(", ")}, all`); quit(2); }
  try {
    CASES[name]();
  } catch (e) {
    check(`case ${name} ran to completion (${e.message})`, false);
    clearAllFailPoints(); // never leave a migration paused
  }
}
finish();
