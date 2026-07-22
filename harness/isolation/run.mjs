// Isolation test runner (HOR-381). Sets up two per-session sandboxes on a
// shared mount (distinct UIDs, 0700 roots, 0711 mount root), then launches the
// probe via the REAL setpriv launcher (dist/launcher.js) under each session UID
// and asserts the kernel enforces isolation.
//
// Sequential-isolation (the warm-worker reuse case): probe A runs under
// UID 1000 in sandbox A, then probe B runs under UID 1001 in sandbox B — a
// fresh process with a different UID/HOME/cwd. Each asserts it cannot read the
// other session's files. (The pi-extension-state bleed variant lands when the
// pi child is wired; this proves the process/UID boundary.)
//
// Then the state-bleed regression (probe-state.mjs): runA mutates
// module/global/timer/descriptor state + persists a PVC marker; runB (fresh
// process) proves zero bleed; resumeA (fresh process) proves only PVC state
// restores, not in-memory state.
//
// Runs as ROOT in a Linux container: the runner needs CAP_SETUID/CAP_SETGID to
// setpriv-spawn children as per-session UIDs + chown the sandboxes. Production
// grants those caps via the K8s security context (HOR-245), NOT by running the
// supervisor as root.

import { execSync } from "node:child_process";
import { launchChild } from "./dist/launcher.js";

const MOUNT = "/data/sandboxes";
const A = `${MOUNT}/A`;
const B = `${MOUNT}/B`;
const UID_A = 1000;
const GID_A = 1000;
const UID_B = 1001;
const GID_B = 1001;

function sh(cmd) {
  execSync(cmd, { stdio: "ignore" });
}

function setup() {
  sh(`rm -rf ${MOUNT}`);
  for (const s of [A, B]) {
    sh(`mkdir -p ${s}/home ${s}/session`);
  }
  // A secret in each sandbox the other must not read.
  sh(`echo "A-secret" > ${A}/session/secret.txt`);
  sh(`echo "B-secret" > ${B}/session/secret.txt`);
  sh(`chown -R ${UID_A}:${GID_A} ${A} && chmod 0700 ${A}`);
  sh(`chown -R ${UID_B}:${GID_B} ${B} && chmod 0700 ${B}`);
  // Mount root: traversable (so a child can reach its own sandbox by known path)
  // but NOT listable (so it cannot enumerate sibling sandbox IDs).
  sh(`chmod 0711 ${MOUNT}`);
}

function runProbe(label, { uid, gid, sandbox, sibling }) {
  const child = launchChild({
    script: "/app/probe.mjs",
    uid,
    gid,
    sandboxRoot: sandbox,
    workingDir: "home",
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      SANDBOX_ROOT: sandbox,
      SIBLING_ROOT: sibling,
      MOUNT_ROOT: MOUNT,
      HOME: `${sandbox}/home`,
    },
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (d) => (stdout += d));
  child.stderr.on("data", (d) => (stderr += d));
  return new Promise((resolve) => {
    child.on("close", (code) => {
      console.log(`\n=== ${label} (uid=${uid}, sandbox=${sandbox}) ===`);
      process.stdout.write(stdout);
      if (stderr) process.stderr.write(stderr);
      console.log(`exit=${code}`);
      resolve(code === 0);
    });
  });
}

setup();

const a = await runProbe("probe A (sandbox=A, sibling=B)", {
  uid: UID_A,
  gid: GID_A,
  sandbox: A,
  sibling: B,
});
const b = await runProbe("probe B (sandbox=B, sibling=A) — fresh process, different UID", {
  uid: UID_B,
  gid: GID_B,
  sandbox: B,
  sibling: A,
});

const pass = a && b;
console.log(`\n=== HOR-381 isolation gate (bullets 1-5): ${pass ? "PASS" : "FAIL"} ===`);
if (!pass) process.exit(1);

// ---- Sequential state-bleed regression (run A -> run B -> resume A) ----
console.log("\n=== HOR-381 sequential state-bleed regression ===");
const runState = (label, sandbox, uid, gid, mode) => {
  const child = launchChild({
    script: "/app/probe-state.mjs",
    uid,
    gid,
    sandboxRoot: sandbox,
    workingDir: "home",
    stdio: ["ignore", "pipe", "pipe"],
    env: { SANDBOX_ROOT: sandbox, MODE: mode, HOME: `${sandbox}/home` },
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (d) => (stdout += d));
  child.stderr.on("data", (d) => (stderr += d));
  return new Promise((resolve) => {
    child.on("close", (code) => {
      console.log(`\n--- ${label} (mode=${mode}, uid=${uid}) exit=${code} ---`);
      process.stdout.write(stdout);
      if (stderr) process.stderr.write(stderr);
      const state = {};
      for (const line of stdout.split("\n")) {
        const m = line.match(/^STATE (\w+)=(.*)$/);
        if (m) state[m[1]] = m[2];
      }
      resolve({ code, state });
    });
  });
};

// A fresh sandbox dir for the state probe (own session subdir).
sh(`mkdir -p ${A}/session ${B}/session`);
sh(`chown -R ${UID_A}:${GID_A} ${A} && chmod 0700 ${A}`);
sh(`chown -R ${UID_B}:${GID_B} ${B} && chmod 0700 ${B}`);

const aRun = await runState("runA", A, UID_A, GID_A, "runA");
const bRun = await runState("runB", B, UID_B, GID_B, "runB");
const aResume = await runState("resumeA", A, UID_A, GID_A, "resumeA");

const bleedPass =
  aRun.code === 0 &&
  bRun.code === 0 &&
  aResume.code === 0 &&
  aRun.state.mark === "A-mutated" && // A mutated in-memory state
  bRun.state.bleed === "no" && // B (fresh process) saw zero bleed
  aResume.state.pvcRestored === "yes" && // PVC marker survived
  aResume.state.memory === "initial" && // in-memory NOT restored
  aResume.state.memoryRestored === "no"; // only PVC state restores

console.log(`\n=== HOR-381 sequential state-bleed regression: ${bleedPass ? "PASS" : "FAIL"} ===`);
process.exit(bleedPass ? 0 : 1);
