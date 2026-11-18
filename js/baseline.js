// Baseline: one ordinary migration with no failpoints and no concurrent writes.
// Shows the phases, how long each took, the log lines each shard printed, and the
// donor's orphaned copy being deleted afterwards.
load(process.env.DEMO_JS_DIR + "/lib.js");

heading("1. Starting state");
resetData();
printOwners();
const before = printCounts("before");

heading("2. Move the chunk");
const since = new Date();
const h = startMigration();
const result = waitForMigration(h);
check("moveRange succeeded", result.res.ok === 1);

heading("3. Ownership and storage right after the commit");
printOwners();
const after = printCounts("after");
check("config server now says recipient owns the range", chunkOwners().some(l => l.startsWith(`[${SPLIT}, MaxKey) on ${RECIPIENT}`)));
check("recipient stores every document in the range", after.recipient === before.donor);
check("donor still physically holds its old copy (orphans)", after.donor === before.donor);
check("queries through mongos see each document once", after.mongos === before.mongos);

heading("4. Phase timings recorded on the config server (config.changelog)");
const cl = mongosConn.getDB("config").changelog;
const fromEntry = cl.find({ns: NS, what: "moveChunk.from", time: {$gte: since}}).sort({time: -1}).limit(1).toArray()[0];
const toEntry = cl.find({ns: NS, what: "moveChunk.to", time: {$gte: since}}).sort({time: -1}).limit(1).toArray()[0];
const commitEntry = cl.find({ns: NS, what: "moveChunk.commit", time: {$gte: since}}).sort({time: -1}).limit(1).toArray()[0];
function printTimings(title, entry, phases, total) {
  print(`\n  ${title}`);
  if (!entry) { print("  (no changelog entry found)"); return; }
  let sum = 0;
  for (let i = 1; i <= total; i++) {
    const ms = entry.details[`step ${i} of ${total}`];
    if (ms === undefined) continue;
    sum += ms;
    print(`    step ${i}  ${String(ms).padStart(6)} ms   ${phases[i]}`);
  }
  print(`    total ${String(sum).padStart(6)} ms`);
}
printTimings("Donor (moveChunk.from)", fromEntry, DONOR_PHASES, 6);
printTimings("Recipient (moveChunk.to)", toEntry, RECIPIENT_PHASES, 8);
if (commitEntry) {
  const c = commitEntry.details.counts || {};
  print(`\n  Documents transferred (moveChunk.commit): cloned=${c.cloned}  catch-up=${c.catchup}  steady=${c.steady}  bytes=${c.clonedBytes}`);
}

heading("5. What each shard logged");
printLogs(`Donor (${DONOR})`, donorConn, since);
printLogs(`Recipient (${RECIPIENT})`, recipientConn, since);

heading(`6. Orphan cleanup (donor deletes its copy after orphanCleanupDelaySecs=${process.env.ORPHAN_DELAY_SECS}s)`);
const deadline = Date.now() + (Number(process.env.ORPHAN_DELAY_SECS) + 120) * 1000;
let lastPrinted = -1;
while (Date.now() < deadline) {
  const n = onDonor().countDocuments({k: {$gte: SPLIT}});
  if (n !== lastPrinted) { say(`donor still stores ${n} documents from the moved range`); lastPrinted = n; }
  if (n === 0) break;
  sleep(1000);
}
const final = printCounts("final");
check("donor's orphans were deleted", final.donor === 0);
check("mongos count unchanged throughout", final.mongos === before.mongos);
printLogs(`Donor range deleter`, donorConn, since, ["RDELETER"]);

finish();
