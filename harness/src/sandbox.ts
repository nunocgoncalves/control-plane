// Sandbox provisioning + validation (HOR-245). The trusted supervisor (root)
// provisions the per-session sandbox on the shared RWX PVC at AssignTurn, then
// validates it before launching the child: resolve the root beneath the
// boot-configured mount root, create+chown it (0700, session UID/GID) if
// missing, verify it exists / is a directory / is NOT a symlink / is owned by
// the session UID/GID / is mode 0700, and resolve the relative working dir.
//
// Provisioning contract (the "separately approved provisioning contract" of
// HOR-381 §4, owned by HOR-245): the provisioner is a strict, audited function
// inside the supervisor — NOT a separate binary — because the supervisor is
// already root and a separate in-pod process would add no real compartment. The
// trust boundary is kernel-enforced (session-UID child vs 0700 sibling roots)
// plus a 0711 root-owned mount root (only root can create sandbox entries, so a
// session-UID child cannot forge a sibling). The provisioner NEVER chowns an
// existing path: a missing root is created; a correctly-provisioned root is
// left untouched (idempotent across turns of the same session); a mismatched
// root is refused with a typed SandboxError → FAILED (never auto-fixed). Repo
// CoW/reflink checkouts remain deferred (HOR-381).

import { lstatSync, mkdirSync, chmodSync, chownSync, rmSync } from "node:fs";
import { isAbsolute, join, resolve, sep } from "node:path";

export class SandboxError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "SandboxError";
  }
}

export interface SandboxPaths {
  root: string;
  home: string;
  tmp: string;
  session: string; // pi JSONL transcript + metadata
  workspace: string; // zero or more task repos/dirs (provisioned by HOR-245)
}

/** Build canonical subpaths under a sandbox root. */
export function sandboxSubpaths(sandboxRoot: string): SandboxPaths {
  return {
    root: sandboxRoot,
    home: join(sandboxRoot, "home"),
    tmp: join(sandboxRoot, "tmp"),
    session: join(sandboxRoot, "session"),
    workspace: join(sandboxRoot, "workspace"),
  };
}

/**
 * Resolve the sandbox root beneath the mount root. The sandbox id must be a
 * single path component (no separators, no traversal) so an AssignTurn can
 * never direct the supervisor outside the mount root.
 */
export function resolveSandboxRoot(sandboxMountRoot: string, sandboxId: string): string {
  if (!sandboxId) throw new SandboxError("sandboxId is required");
  if (sandboxId.includes("/") || sandboxId.includes(sep) || sandboxId === "." || sandboxId === "..")
    throw new SandboxError(`sandboxId must be a single path component (got ${JSON.stringify(sandboxId)})`);
  return join(sandboxMountRoot, sandboxId);
}

/**
 * Validate a sandbox root and return its canonical subpaths. Checks: exists,
 * is a directory, is NOT a symlink, owned by (uid, gid), mode 0700. Does not
 * chown and does not auto-create — a missing/mismatched root is a typed error.
 * Run after {@link provisionSandbox} as the post-provision integrity gate
 * before spawning the child.
 */
export function validateSandbox(sandboxRoot: string, uid: number, gid: number): SandboxPaths {
  assertOwnedDir(sandboxRoot, uid, gid, "sandbox root");
  return sandboxSubpaths(sandboxRoot);
}

/**
 * Provision (create + chown) a missing per-session sandbox root + its canonical
 * subdirectories, or assert an existing one is already correctly provisioned
 * (idempotent across turns of the same session). Returns the canonical paths.
 *
 * Trust contract:
 *  - Missing root → create root + home/tmp/session/workspace at mode 0700,
 *    chowned to the assignment's (uid, gid). Only the supervisor (root) can
 *    create entries under the 0711 mount root, so a session-UID child cannot
 *    pre-create a sandbox to be adopted.
 *  - Existing root → idempotent ONLY if the root + every subdir is already a
 *    non-symlink directory owned by (uid, gid) at mode 0700. Otherwise it is a
 *    typed SandboxError → FAILED. The provisioner NEVER chowns or "completes"
 *    an existing mismatched/partial path (a crash mid-provision leaves a root
 *    the next provision refuses; v1 accepts FAILED, HOR-381).
 *  - (uid, gid) come from the durable assignment (AssignTurn), validated by the
 *    supervisor — never child-supplied.
 *
 * Call validateSandbox afterwards (the supervisor does) as the post-provision
 * integrity gate before spawning the child.
 */
export function provisionSandbox(sandboxRoot: string, uid: number, gid: number): SandboxPaths {
  const paths = sandboxSubpaths(sandboxRoot);
  let rootExists = false;
  try {
    lstatSync(sandboxRoot);
    rootExists = true;
  } catch {
    rootExists = false; // ENOENT (or other) → treat as absent; mkdir surfaces real errors
  }
  if (!rootExists) {
    // Create root + canonical subdirs at 0700, chowned to the session UID/GID.
    // chmod defeats the process umask; chown happens before the child is spawned.
    for (const p of [paths.root, paths.home, paths.tmp, paths.session, paths.workspace]) {
      mkdirSync(p, { mode: 0o700 });
      chmodSync(p, 0o700);
      chownSync(p, uid, gid);
    }
    return paths;
  }
  // Root exists: idempotent only if root + every subdir is already correctly
  // provisioned for THIS session. NEVER chown/complete a mismatched path.
  assertOwnedDir(paths.root, uid, gid, "sandbox root");
  assertOwnedDir(paths.home, uid, gid, "sandbox home");
  assertOwnedDir(paths.tmp, uid, gid, "sandbox tmp");
  assertOwnedDir(paths.session, uid, gid, "sandbox session");
  assertOwnedDir(paths.workspace, uid, gid, "sandbox workspace");
  return paths;
}

/**
 * Reap (recursively remove) a terminated session's sandbox from the shared RWX
 * PVC. Called by the supervisor on `SessionEnd` (the HOR-245 cleanup owner —
 * the supervisor that provisioned the sandbox also reaps it). Symmetric with
 * {@link provisionSandbox} and the same trust boundary applies: only the
 * supervisor (root) can modify entries under the 0711 root-owned mount root,
 * so a session-UID child cannot swap the sandbox root for a symlink or forge a
 * sibling to be deleted.
 *
 * Safety contract:
 *  - Missing root → no-op (idempotent: never provisioned, or already reaped).
 *  - Symlink / non-directory root → typed SandboxError (refused; never follows).
 *  - Root owned by a (uid, gid) other than the session's → typed SandboxError
 *    (refused; never reaps a foreign/mismatched path). (uid, gid) come from the
 *    durable SessionEnd, never child-supplied.
 *  - Removal is recursive but does NOT follow symlinks inside the sandbox: a
 *    child-owned symlink entry is unlinked, never its target. The root itself
 *    cannot be swapped (0711 root-owned parent), so there is no root-level
 *    TOCTOU.
 *  - After removal, the root is re-stat'd; if it persists (e.g. a foreign file
 *    was recreated, or EPERM on a root-squashed volume) → typed SandboxError so
 *    the leak is surfaced rather than silently accepted.
 *
 * Reaping is best-effort relative to dispatch state: the CP MUST NOT recycle a
 * sandbox_id before the worker has ACKed reaping (v1: the CP simply waits for
 * the next Ready / a bounded grace). A transient IO error is surfaced as a
 * SandboxError so the supervisor can fail-closed rather than leak silently.
 */
export function reapSandbox(sandboxRoot: string, uid: number, gid: number): void {
  let st;
  try {
    st = lstatSync(sandboxRoot);
  } catch (err) {
    // Missing root: idempotent no-op (never provisioned or already reaped).
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
    throw new SandboxError(`reap stat failed for ${sandboxRoot}: ${(err as Error).message}`);
  }
  if (st.isSymbolicLink()) throw new SandboxError(`sandbox root is a symlink (refused): ${sandboxRoot}`);
  if (!st.isDirectory()) throw new SandboxError(`sandbox root is not a directory (refused): ${sandboxRoot}`);
  if (st.uid !== uid || st.gid !== gid)
    throw new SandboxError(`sandbox root owned by ${st.uid}:${st.gid}, not session ${uid}:${gid} (refused): ${sandboxRoot}`);
  try {
    rmSync(sandboxRoot, { recursive: true, force: false });
  } catch (err) {
    throw new SandboxError(`reap failed for ${sandboxRoot}: ${(err as Error).message}`);
  }
  // VERIFY: the root is gone. A root-squashed/EPERM volume may silently ignore
  // the removal; surface the leak rather than accept it.
  try {
    lstatSync(sandboxRoot);
    throw new SandboxError(`sandbox root persists after reap (refused): ${sandboxRoot}`);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
  }
}

/**
 * Ensure the sandbox mount root (the shared RWX PVC) is a non-symlink directory
 * owned by the supervisor at mode 0711: traversable (so a session-UID child
 * can reach its own 0700 root) but not listable/writable by non-root (so only
 * the supervisor can create sandbox entries — a child cannot forge a sibling).
 * Called once at supervisor startup.
 *
 * ESTABLISH + VERIFY: the mode/ownership are set and then RE-STAT'd — a
 * root-squashed or pre-owned RWX volume can silently ignore chmod/chown, so the
 * resulting inode (not the call) is the source of truth. Startup FAILS if a
 * safe root cannot be guaranteed (symlink attack, foreign owner, un-fixable
 * mode). It never silently degrades to a best-effort skip: a writable/listable
 * parent lets a session-UID child create, rename, or delete sibling roots
 * regardless of each sibling's 0700 mode (HOR-245/HOR-381 isolation contract).
 */
export function ensureSandboxMountRoot(mountRoot: string): void {
  // Create if missing (recursive so intermediate dirs exist).
  try {
    mkdirSync(mountRoot, { mode: 0o711, recursive: true });
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
  }

  // Reject a symlink root before any chmod/chown (TOCTOU-safe: lstat, not stat).
  let st = lstatSync(mountRoot);
  if (st.isSymbolicLink()) throw new SandboxError(`sandbox mount root is a symlink (refused): ${mountRoot}`);
  if (!st.isDirectory()) throw new SandboxError(`sandbox mount root is not a directory: ${mountRoot}`);

  // The supervisor must have a determinable uid/gid (Linux). On a platform
  // without getuid the ownership invariant is undefined — refuse to start.
  if (typeof process.getuid !== "function" || typeof process.getgid !== "function") {
    throw new SandboxError(`supervisor uid/gid unavailable on this platform (refused): ${mountRoot}`);
  }
  const uid = process.getuid();
  const gid = process.getgid();
  // Establish ownership: the supervisor must own the root so only it can create
  // sandbox entries. As root we (re)claim a stray/foreign-owned volume; as
  // non-root we can only own what we created — a foreign owner is refused.
  if (st.uid !== uid || st.gid !== gid) {
    if (uid === 0) {
      chownSync(mountRoot, 0, 0);
    } else {
      throw new SandboxError(`sandbox mount root owned by ${st.uid}:${st.gid}, not supervisor ${uid}:${gid} (refused): ${mountRoot}`);
    }
  }

  // Establish mode 0711. EPERM (root-squash, read-only mount) means we cannot
  // guarantee the mode — fail startup rather than run with a writable parent.
  try {
    chmodSync(mountRoot, 0o711);
  } catch (err) {
    throw new SandboxError(`cannot chmod sandbox mount root to 0711 (${(err as NodeJS.ErrnoException).code}): ${mountRoot}`);
  }

  // VERIFY: re-stat and assert the final type/owner/mode. Never trust the
  // chmod/chown call on a volume that may silently ignore it.
  st = lstatSync(mountRoot);
  if (st.isSymbolicLink()) throw new SandboxError(`sandbox mount root is a symlink (refused): ${mountRoot}`);
  if (!st.isDirectory()) throw new SandboxError(`sandbox mount root is not a directory: ${mountRoot}`);
  if (st.uid !== uid || st.gid !== gid) throw new SandboxError(`sandbox mount root owner ${st.uid}:${st.gid} != supervisor ${uid}:${gid}: ${mountRoot}`);
  if ((st.mode & 0o777) !== 0o711) throw new SandboxError(`sandbox mount root mode ${(st.mode & 0o777).toString(8)} != 0711: ${mountRoot}`);
}

/**
 * Assert `p` is a non-symlink directory owned by (uid, gid) at mode 0700, else
 * throw a typed SandboxError. Shared by validateSandbox and the idempotent path
 * of provisionSandbox.
 */
function assertOwnedDir(p: string, uid: number, gid: number, label: string): void {
  let st;
  try {
    st = lstatSync(p);
  } catch {
    throw new SandboxError(`${label} missing (not provisioned): ${p}`);
  }
  if (st.isSymbolicLink()) throw new SandboxError(`${label} is a symlink (refused): ${p}`);
  if (!st.isDirectory()) throw new SandboxError(`${label} is not a directory: ${p}`);
  if (st.uid !== uid) throw new SandboxError(`${label} uid ${st.uid} != expected ${uid}: ${p}`);
  if (st.gid !== gid) throw new SandboxError(`${label} gid ${st.gid} != expected ${gid}: ${p}`);
  const mode = st.mode & 0o777;
  if (mode !== 0o700) throw new SandboxError(`${label} mode ${mode.toString(8)} != 0700: ${p}`);
}

/**
 * Resolve a relative working directory within the sandbox root. Rejects
 * absolute paths and any traversal that escapes the root. Returns the absolute
 * cwd the child runs under.
 */
export function resolveWorkingDir(sandboxRoot: string, workingDir: string): string {
  if (!workingDir) throw new SandboxError("workingDir is required");
  if (isAbsolute(workingDir)) throw new SandboxError("workingDir must be relative to the sandbox root");
  const root = resolve(sandboxRoot);
  const target = resolve(root, workingDir);
  if (target !== root && !target.startsWith(root + sep))
    throw new SandboxError(`workingDir escapes the sandbox root: ${workingDir}`);
  return target;
}
