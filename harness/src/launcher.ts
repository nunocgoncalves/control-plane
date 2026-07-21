// The setpriv-based privilege-dropping launcher (HOR-381).
//
// Spawns the per-turn pi child under a stable per-session UID/GID with
// no_new_privs and ALL Linux capabilities dropped (bounding + inheritable +
// ambient cleared by setpriv; permitted + effective cleared by setuid). The
// supervisor — which HOR-245's security context grants only CAP_SETUID /
// CAP_SETGID — uses this to run model-directed code (pi + extensions) with
// kernel-enforced isolation: the child can read/write its own 0700 sandbox but
// receives EACCES for any sibling session root on the shared RWX PVC.
//
// setpriv (util-linux) is the production launcher. The full drop is empirically
// verified in node:24-bookworm-slim (util-linux 2.38.1) — see harness/isolation.
// A Go launcher is the fallback ONLY if the isolation tests prove setpriv
// insufficient under the target security context.
//
// Owned by HOR-381 (NOT HOR-245, which owns only the K8s security context that
// grants the supervisor caps + sandbox provisioning).

import { spawn, type ChildProcess, type SpawnOptions, type StdioOptions } from "node:child_process";
import { isAbsolute, join, resolve, sep } from "node:path";

/** Path to the setpriv binary (util-linux). Overridable via env for tests. */
export const SETPRIV_BIN = process.env.HARNESS_SETPRIV_BIN ?? "setpriv";

export interface LaunchOptions {
  /** Absolute path to the child Node script (the pi child entry point). */
  script: string;
  /** Stable per-session UID (control-plane-assigned; the provisioner chowns the sandbox to it). */
  uid: number;
  /** Stable per-session GID. */
  gid: number;
  /** Absolute sandbox root (the supervisor validates ownership/mode before launch). */
  sandboxRoot: string;
  /**
   * Working directory relative to sandboxRoot (validated; never absolute or
   * escaping). The child's cwd = join(sandboxRoot, workingDir).
   */
  workingDir: string;
  /** stdio for the child: the supervisor passes inherited IPC pipes + piped stdout/stderr. */
  stdio: StdioOptions;
  /** Env for the child. `--reset-env` drops the supervisor's env; only this is passed. */
  env: NodeJS.ProcessEnv;
}

export class LauncherError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "LauncherError";
  }
}

/**
 * Build the setpriv argv that drops to `uid`/`gid` with no_new_privs and a
 * fully-cleared capability set, then execs `node <script>`.
 *
 *   setpriv --reuid <uid> --regid <gid> --clear-groups --no-new-privs \
 *           --bounding-set -all --inh-caps -all --ambient-caps -all \
 *           <node-execPath> <script>
 *
 * Env isolation comes from spawn({env: childEnv}) — Node replaces (not
 * merges) the env, so only the supervisor-constructed child env reaches the
 * child. `--reset-env` is deliberately NOT used: it would wipe that env.
 * `<node-execPath>` is `process.execPath` (absolute), so the dropped-env
 * child (which has no PATH) can still exec node.
 */
export function buildSetprivArgv(opts: LaunchOptions): string[] {
  validateLaunchOptions(opts);
  return [
    "--reuid",
    String(opts.uid),
    "--regid",
    String(opts.gid),
    "--clear-groups",
    "--no-new-privs",
    "--bounding-set",
    "-all",
    "--inh-caps",
    "-all",
    "--ambient-caps",
    "-all",
    process.execPath,
    opts.script,
  ];
}

/** Build the spawn options: cwd inside the validated sandbox + the child env + stdio. */
export function buildSpawnOptions(opts: LaunchOptions): SpawnOptions {
  validateLaunchOptions(opts);
  return {
    cwd: join(opts.sandboxRoot, opts.workingDir),
    stdio: opts.stdio,
    env: opts.env,
  };
}

/**
 * Spawn the per-turn child via setpriv under the session UID/GID, with cwd
 * inside the validated sandbox. The returned ChildProcess owns the IPC pipes
 * and stdout/stderr log streams passed via `stdio`.
 */
export function launchChild(opts: LaunchOptions): ChildProcess {
  return spawn(SETPRIV_BIN, buildSetprivArgv(opts), buildSpawnOptions(opts));
}

/**
 * Minimal input validation. Ownership/mode/cwd-sandboxing of the sandbox root
 * itself is `sandbox.ts`'s job (a later increment); this only guards the
 * launcher's own inputs: positive integer UID/GID, absolute script + sandbox
 * root, and a relative workingDir that does not escape the sandbox root.
 */
export function validateLaunchOptions(opts: LaunchOptions): void {
  requireNonEmpty(opts.script, "script");
  if (!isAbsolute(opts.script)) throw new LauncherError("script must be an absolute path");
  if (!Number.isInteger(opts.uid) || opts.uid <= 0)
    throw new LauncherError(`uid must be a positive integer (got ${opts.uid})`);
  if (!Number.isInteger(opts.gid) || opts.gid <= 0)
    throw new LauncherError(`gid must be a positive integer (got ${opts.gid})`);
  requireNonEmpty(opts.sandboxRoot, "sandboxRoot");
  if (!isAbsolute(opts.sandboxRoot)) throw new LauncherError("sandboxRoot must be an absolute path");
  requireNonEmpty(opts.workingDir, "workingDir");
  if (isAbsolute(opts.workingDir)) throw new LauncherError("workingDir must be relative to sandboxRoot");
  const root = resolve(opts.sandboxRoot);
  const target = resolve(root, opts.workingDir);
  if (target !== root && !target.startsWith(root + sep))
    throw new LauncherError(`workingDir escapes sandboxRoot: ${opts.workingDir}`);
}

function requireNonEmpty(v: string | undefined | null, name: string): void {
  if (v === undefined || v === null || v === "") throw new LauncherError(`${name} is required`);
}
