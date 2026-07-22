// HOR-381 sequential-isolation state-bleed probe (the warm-worker reuse case).
//
// The warm-worker runs each turn in a FRESH pi child process under a per-session
// UID/GID. The isolation guarantee for sequential reuse (run A -> dispose -> run
// B -> resume A) is the PROCESS BOUNDARY: a fresh process has a fresh extension
// heap — no module globals, no timers, no open descriptors bleed from a prior
// turn. Only PVC (filesystem) state — the pi session transcript — survives,
// restored on resume.
//
// This probe mutates the four state categories the HOR-381 gate names
// (module/global/timer/descriptor state) inside the child process and asserts:
//   runA   : mutates state, persists a PVC marker, reports the in-memory mark.
//   runB   : fresh process -> state is at its initial values (ZERO bleed from A).
//   resumeA: fresh process -> the PVC marker is restored from disk, but the
//            in-memory state is fresh (NOT restored) -> only PVC state survives.
//
// Runs under the REAL setpriv launcher (dist/launcher.js) as a per-session UID,
// so the kernel process/UID boundary is what enforces zero bleed. Node built-ins
// only (no pi SDK) — the property under test is the process boundary the harness
// depends on, not pi internals.
//
// Env: SANDBOX_ROOT, MODE=runA|runB|resumeA. Prints `STATE <key>=<value>` lines.

import fs from "node:fs";

const SANDBOX = process.env.SANDBOX_ROOT;
const MODE = process.env.MODE;
const PVC = `${SANDBOX}/session/state.json`;

if (!SANDBOX || !MODE) {
  console.error("probe-state: SANDBOX_ROOT and MODE env vars are required");
  process.exit(2);
}

// ---- a deliberately stateful "extension" module (per-process state) ----
//
// Each field models a category of in-process state a pi extension can hold:
//   mark      : module-global mutable variable
//   timer     : a live setInterval (kept referenced so the loop stays alive
//               until the process exits — simulating a long-lived extension timer)
//   descriptor: an open file descriptor / handle held by the module
const state = (() => {
  let mark = "initial"; // module-global
  let timer = null; // module-held timer
  let descriptor = null; // module-held open fd
  return {
    getMark: () => mark,
    mutate: (v) => {
      mark = v;
      // a live timer that does nothing but exist (would bleed if shared)
      timer = setInterval(() => {}, 1 << 30);
      // an open descriptor held by the module
      descriptor = fs.openSync(`${SANDBOX}/session/.descriptor`, "w");
    },
    hasTimer: () => timer !== null,
    hasDescriptor: () => descriptor !== null,
  };
})();

const out = [];
const emit = (k, v) => out.push(`STATE ${k}=${v}`);

if (MODE === "runA") {
  // Mutate every state category, then persist a PVC marker and report.
  state.mutate("A-mutated");
  fs.writeFileSync(
    PVC,
    JSON.stringify({ mark: state.getMark(), sessionMarker: "A" }),
  );
  emit("mode", "runA");
  emit("mark", state.getMark()); // A-mutated (in-memory)
  emit("timer", state.hasTimer() ? "yes" : "no");
  emit("descriptor", state.hasDescriptor() ? "yes" : "no");
  emit("pvc", "written");
  // "dispose": the process exits here. The timer, descriptor, and module-global
  // are destroyed with the process; only the PVC marker file remains on disk.
} else if (MODE === "runB") {
  // Fresh process: state must be at its initial values — no bleed from A.
  emit("mode", "runB");
  emit("mark", state.getMark()); // initial (zero bleed)
  emit("timer", state.hasTimer() ? "yes" : "no"); // no
  emit("descriptor", state.hasDescriptor() ? "yes" : "no"); // no
  emit("bleed", state.getMark() === "initial" && !state.hasTimer() && !state.hasDescriptor() ? "no" : "yes");
} else if (MODE === "resumeA") {
  // Fresh process resuming A's session: ONLY PVC state restores.
  let pvcMark = "(missing)";
  let pvcRestored = false;
  try {
    const persisted = JSON.parse(fs.readFileSync(PVC, "utf8"));
    pvcMark = persisted.mark;
    pvcRestored = true;
  } catch {
    pvcRestored = false;
  }
  emit("mode", "resumeA");
  emit("pvc", pvcMark); // A-mutated (restored from disk)
  emit("pvcRestored", pvcRestored ? "yes" : "no");
  emit("memory", state.getMark()); // initial (fresh process — NOT restored)
  emit("timer", state.hasTimer() ? "yes" : "no"); // no
  emit("memoryRestored", state.getMark() === "A-mutated" ? "yes" : "no"); // no
} else {
  console.error(`probe-state: unknown MODE ${MODE}`);
  process.exit(2);
}

console.log(out.join("\n"));
process.exit(0);
